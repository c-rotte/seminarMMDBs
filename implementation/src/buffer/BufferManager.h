#ifndef BTREE_BUFFERMANAGER_H
#define BTREE_BUFFERMANAGER_H
// --------------------------------------------------------------------------
#include "DiskManager.h"
#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <iostream>
// --------------------------------------------------------------------------
namespace buffer {
// --------------------------------------------------------------------------
template <size_t PAGE_SIZE>
struct Page {
    uint64_t id;
    // contention detection
    size_t updates;
    size_t slowPaths;
    size_t lastUpdatesPos;
    // buffer
    std::atomic<size_t> pinned;
    std::atomic<bool> referenced;
    std::atomic<bool> modified;
    std::atomic<bool> deleted;
    std::shared_mutex mutex;
    // frame
    disk::Frame<PAGE_SIZE> frame;
    Page(uint64_t, disk::Frame<PAGE_SIZE>);
};
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
class BufferManager {

    private:
#ifdef LOGGING
    public:
#endif
    disk::DiskManager<PAGE_SIZE> diskManager;
    // clock
    size_t hand;
    std::unordered_map<uint64_t, size_t> loadedPages;
    std::unordered_set<uint64_t> innerNodes;

    std::array<std::unique_ptr<Page<PAGE_SIZE>>, PAGE_AMOUNT> buffer;
    mutable std::shared_mutex mutex;
    using BeforeLoadingFunc = std::function<bool(
        uint64_t,
        std::unordered_map<uint64_t, size_t>&,
        std::unordered_set<uint64_t>&,
        std::array<std::unique_ptr<Page<PAGE_SIZE>>, PAGE_AMOUNT>&,
        disk::DiskManager<PAGE_SIZE>&)>;
    BeforeLoadingFunc beforeEvictingFunc;
    using IsInnerNodeFunc = std::function<bool(Page<PAGE_SIZE>*)>;
    IsInnerNodeFunc isInnerNodeFunc;

// enable logging
#ifdef LOGGING
    public:
    std::atomic<size_t> SWAPS = 0;
    std::atomic<size_t> SPECIAL_LOADS = 0;
#endif

    public:
    explicit BufferManager(const std::string&,
                           BeforeLoadingFunc beforeEvictingFunc = nullptr,
                           IsInnerNodeFunc isInnerNodeFunc = nullptr);
    ~BufferManager();

    private:
    bool loadIntoMemory(uint64_t, bool);

    public:
    size_t totalFrames() const;
    Page<PAGE_SIZE>* pinPage(uint64_t, bool initializedNode = false);
    void unpinPage(uint64_t, bool);
    uint64_t newPage();
    bool deletePage(uint64_t);
};
// --------------------------------------------------------------------------
template <size_t PAGE_SIZE>
Page<PAGE_SIZE>::Page(uint64_t id, disk::Frame<PAGE_SIZE> frame)
    : id(id), updates(0), slowPaths(0), lastUpdatesPos(0), pinned(0),
      referenced(true), modified(false), deleted(false), frame(std::move(frame)) {
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
BufferManager<PAGE_AMOUNT, PAGE_SIZE>::BufferManager(
    const std::string& filePath, BeforeLoadingFunc beforeEvictingFunc, IsInnerNodeFunc isInnerNodeFunc)
    : diskManager(filePath), hand(0),
      beforeEvictingFunc(std::move(beforeEvictingFunc)), isInnerNodeFunc(std::move(isInnerNodeFunc)) {
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
BufferManager<PAGE_AMOUNT, PAGE_SIZE>::~BufferManager() {
    for (auto& page : buffer) {
        if (!page) {
            continue;
        }
        if (page->deleted) {
            diskManager.deletePage(page->id);
        } else if (page->modified) {
            diskManager.writePage(page->id, page->frame);
        }
    }
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
bool BufferManager<PAGE_AMOUNT, PAGE_SIZE>::loadIntoMemory(uint64_t id, bool initializedNode) {
    // function to load page
    const static auto loadPage = [](BufferManager<PAGE_AMOUNT, PAGE_SIZE>& tree,
                                    uint64_t id, bool initializedNode) {
        auto page = std::make_unique<Page<PAGE_SIZE>>(
            id, std::move(tree.diskManager.retrievePage(id)));
        auto* pagePointer = page.get();
        // store it
        tree.buffer[tree.hand] = std::move(page);
        tree.loadedPages[id] = tree.hand;
        //
        tree.hand = (tree.hand + 1) % PAGE_AMOUNT;
        if(initializedNode && tree.isInnerNodeFunc && id != 0){
            if(tree.isInnerNodeFunc(pagePointer)){
                tree.innerNodes.insert(id);
            }
        }
    };
    size_t encounters = 0;
    bool foundUnpinned = false;
    while (!(encounters >= PAGE_AMOUNT && !foundUnpinned)) {
        if (!buffer[hand]) {
            // empty, can be used
            loadPage(*this, id, initializedNode);
            return true;
        } else if (buffer[hand]->pinned == 0) {
            if (buffer[hand]->deleted || !buffer[hand]->referenced) {
                // a page will be evicted; try out the custom
                // loading strategy (x-merge) before that
                if (beforeEvictingFunc &&
                    beforeEvictingFunc(
                        id, loadedPages, innerNodes, buffer, diskManager)) {
#ifdef LOGGING
                    SPECIAL_LOADS++;
#endif
                    return true;
                }
            }
            foundUnpinned = true;
            if (buffer[hand]->deleted) {
                // page was deleted, can be used
                auto& p = *buffer[hand];
                diskManager.deletePage(p.id);
                // use it
                loadPage(*this, id, initializedNode);
                return true;
            } else if (!buffer[hand]->referenced) {
                // not referenced, can be used
                auto& p = *buffer[hand];
                // write back if modified
                if (p.modified) {
                    diskManager.writePage(p.id, p.frame);
                }
                loadedPages.erase(p.id);
                innerNodes.erase(p.id); // does nothing if it's not an inner node
                // use it
                loadPage(*this, id, initializedNode);
#ifdef LOGGING
                SWAPS++;
#endif
                return true;
            } else {
                // second chance
                buffer[hand]->referenced = false;
            }
        }
        encounters++;
        hand = (hand + 1) % PAGE_AMOUNT;
    }
    return false;
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
size_t BufferManager<PAGE_AMOUNT, PAGE_SIZE>::totalFrames() const {
    return diskManager.entryAmount();
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
Page<PAGE_SIZE>* BufferManager<PAGE_AMOUNT, PAGE_SIZE>::pinPage(uint64_t id, bool initializedNode) {
    {
        std::shared_lock lock(mutex);
        // check if the page is in memory
        if (loadedPages.contains(id)) {
            auto* page = buffer[loadedPages[id]].get();
            page->pinned++;
            page->referenced = true;
            return page;
        }
        // destructor unlocks lock
    }
    // request exclusive permissions
    std::unique_lock lock(mutex);
    // check again if the page is in memory
    if (loadedPages.contains(id)) {
        auto* page = buffer[loadedPages[id]].get();
        page->pinned++;
        page->referenced = true;
        return page;
    }
    // load into memory
    if (!loadIntoMemory(id, initializedNode)) {
        return nullptr;
    }
    auto* page = buffer[loadedPages[id]].get();
    page->pinned++;
    return page;
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
void BufferManager<PAGE_AMOUNT, PAGE_SIZE>::unpinPage(uint64_t id, bool modified) {
    std::shared_lock lock(mutex);
    // check if the page is still in memory
    if (!loadedPages.contains(id)) {
        return;
    }
    auto& page = *buffer[loadedPages[id]];
    if (modified) {
        page.modified = true;
    }
    assert(page.pinned != 0);
    page.pinned--;
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
uint64_t BufferManager<PAGE_AMOUNT, PAGE_SIZE>::newPage() {
    uint64_t id = diskManager.createPage().first;
    return id;
}
// --------------------------------------------------------------------------
template <size_t PAGE_AMOUNT, size_t PAGE_SIZE>
bool BufferManager<PAGE_AMOUNT, PAGE_SIZE>::deletePage(uint64_t id) {
    std::unique_lock lock(mutex);
    // check if the page is in memory
    if (loadedPages.contains(id)) {
        auto& page = *buffer[loadedPages[id]];
        // only unpinned pages may be deleted
        if (page.pinned == 0) {
            page.deleted = true;
            loadedPages.erase(page.id);
            innerNodes.erase(page.id);
            return true;
        }
        return false;
    }
    diskManager.deletePage(id);
    return true;
}
// --------------------------------------------------------------------------
} // namespace buffer
// --------------------------------------------------------------------------
#endif //BTREE_BUFFERMANAGER_H
