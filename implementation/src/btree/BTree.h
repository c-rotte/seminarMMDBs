#ifndef BTREE_BTREE_H
#define BTREE_BTREE_H
// --------------------------------------------------------------------------
#include "src/buffer/BufferManager.h"
#include "src/buffer/DiskManager.h"
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <functional>
#include <mutex>
#include <new>
#include <optional>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
// --------------------------------------------------------------------------
namespace btree {
// --------------------------------------------------------------------------
template <class KEY, size_t DEGREE>
requires std::totally_ordered<KEY>
struct Node {
    bool leaf;
    uint32_t keyAmount;
    std::array<KEY, DEGREE> keys;
    std::array<uint64_t, DEGREE + 1> children;
};
// --------------------------------------------------------------------------
template <class KEY, size_t TOTAL_PAGE_SIZE>
// there must be place for at least one key
concept ValidPageSize =
    TOTAL_PAGE_SIZE >= sizeof(buffer::Page<0>) + sizeof(Node<KEY, 0>) + sizeof(KEY) + 2 * sizeof(uint64_t);
// --------------------------------------------------------------------------
template <class KEY, size_t TOTAL_PAGE_SIZE>
// maximal storage for (key,children) pairs = total - size of empty page - size of
// empty node - the size of the last children
static constexpr size_t DEGREE =
    (TOTAL_PAGE_SIZE - sizeof(buffer::Page<0>) - sizeof(Node<KEY, 0>) - sizeof(uint64_t)) /
    (sizeof(KEY) + sizeof(uint64_t));
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
class BTree {
    public:
    // fit as much node slots as possible into each buffer page
    static constexpr size_t PAGE_SIZE = sizeof(Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>);
    static constexpr size_t DATA_PAGE_SIZE = sizeof(DATA);
    static constexpr size_t KEYS_PER_NODE = DEGREE<KEY, TOTAL_PAGE_SIZE> - 1;
    // page must fit in the provided memory
    static_assert(TOTAL_PAGE_SIZE >= sizeof(buffer::Page<PAGE_SIZE>));
    // nodes and data must be properly aligned (to be stored in frames)
    static_assert(alignof(Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>) <= alignof(disk::Frame<PAGE_SIZE>));
    static_assert(alignof(DATA) <= alignof(disk::Frame<PAGE_SIZE>));

    private:
#ifdef LOGGING
    public:
    std::atomic<size_t> CONTENTION_SPLITS = 0;
#endif
    public:
    double d1 = 0.05;
    double d2 = 0.01;
    double d3 = 0.8;

    const bool contentionSplitEnabled;
    // tree nodes
    buffer::BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager;
    // tuples
    disk::DiskManager<sizeof(DATA)> diskManager;
    // b+-tree
    uint64_t root;

    public:
    BTree(const std::string&, const std::string&, bool, bool);

    private:
    void initializeNode(buffer::Page<PAGE_SIZE>&) const;
    static Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>& getNode(buffer::Page<PAGE_SIZE>&);
    static DATA& getData(disk::Frame<sizeof(DATA)>&);
    size_t findChildrenIndex(Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>&, const KEY&) const;
    // node n will be split into left (new) and right (n); where the key at
    // index i will be stored in the right one
    uint64_t split(Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>&, size_t);
    void simpleInsert(Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>&, size_t, KEY, uint64_t) const;
    // returns (tried, success); assumes that the parent is shared and
    // the child is locked exclusively
    std::pair<bool, bool> tryContentionSplit(buffer::Page<PAGE_SIZE>&,
                                             buffer::Page<PAGE_SIZE>&, bool, size_t, const KEY&);
    void insert(uint64_t, KEY, DATA);

    public:
    static bool isInnerNode(buffer::Page<PAGE_SIZE>*);
    static bool tryXMerge(uint64_t,
                          std::unordered_map<uint64_t, size_t>&,
                          std::unordered_set<uint64_t>&,
                          std::array<std::unique_ptr<buffer::Page<PAGE_SIZE>>, PAGE_AMOUNT>&,
                          disk::DiskManager<PAGE_SIZE>&);
    size_t size() const;
    std::optional<DATA> find(const KEY&);
    void insert(KEY, DATA);
    bool contains(const KEY&);
    bool update(const KEY&, const std::function<void(DATA&)>&);
    // not thread safe
    void print(uint64_t, bool);
};
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::BTree(
    const std::string& treePath, const std::string& dataPath, bool contentionSplitEnabled, bool xMergeEnabled)
    : contentionSplitEnabled(contentionSplitEnabled),
      bufferManager(treePath, !xMergeEnabled ? nullptr : tryXMerge, isInnerNode),
      diskManager(dataPath), root(0) {
    // the tree always contains at least a root node
    if (bufferManager.totalFrames() == 0) {
        root = bufferManager.newPage();
        buffer::Page<PAGE_SIZE>* page;
        while (!(page = bufferManager.pinPage(root)))
            ;
        initializeNode(*page);
        bufferManager.unpinPage(root, true);
    }
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
void BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::initializeNode(buffer::Page<PAGE_SIZE>& page) const {
    // create a new node using placement new
    auto* node = new (page.frame.content.data()) Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>;
    node->leaf = true;
    node->keyAmount = 0;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
    Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>
&BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::getNode(buffer::Page<PAGE_SIZE>& page) {
    // reinterpret the content (defined behaviour since the frame and its data array are properly aligned)
    auto* ptr = reinterpret_cast<Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>*>(page.frame.content.data());
    return *ptr;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
    DATA& BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::getData(disk::Frame<sizeof(DATA)>& frame) {
    // reinterpret the content (defined behaviour since the frame and its data array are properly aligned)
    auto* ptr = reinterpret_cast<DATA*>(frame.content.data());
    return *ptr;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
size_t BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::findChildrenIndex(
    Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>& node, const KEY& key) const {
    for (size_t i = 0; i < node.keyAmount; i++) {
        if (key < node.keys[i]) {
            return i;
        }
    }
    return node.keyAmount;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
uint64_t BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::split(
    Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>& node, size_t index) {
    // create a new page
    const uint64_t leftID = bufferManager.newPage();
    buffer::Page<PAGE_SIZE>* leftPage;
    while (!(leftPage = bufferManager.pinPage(leftID)))
        ;
    initializeNode(*leftPage);
    auto& leftNode = getNode(*leftPage);
    leftNode.leaf = node.leaf;
    // treat the left node as right node
    // move keys and pointers to the right node
    std::move(std::begin(node.keys) + index, std::end(node.keys), std::begin(leftNode.keys));
    std::move(std::begin(node.children) + index, std::end(node.children), std::begin(leftNode.children));
    leftNode.keyAmount = node.keyAmount - index;
    node.keyAmount = index;
    assert(leftNode.keyAmount <= leftNode.keys.size());
    assert(node.keyAmount <= node.keys.size());
    // swap the nodes
    std::swap(node, leftNode);
    bufferManager.unpinPage(leftID, true);
    return leftID;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
void BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::
    simpleInsert(Node<KEY, DEGREE<KEY, TOTAL_PAGE_SIZE>>& node, size_t index, KEY key, uint64_t id) const {
    // move all greater entries to the right
    assert(index < node.keys.size());
    std::move(std::begin(node.keys) + index, std::end(node.keys) - 1, std::begin(node.keys) + index + 1);
    std::move(std::begin(node.children) + index, std::end(node.children) - 1, std::begin(node.children) + index + 1);
    // insert
    node.keys[index] = std::move(key);
    node.children[index] = id;
    node.keyAmount++;
    assert(node.keyAmount <= node.keys.size());
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
std::pair<bool, bool> BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::
    tryContentionSplit(buffer::Page<PAGE_SIZE>& parentPage, buffer::Page<PAGE_SIZE>& currentPage,
                       bool fastPath, size_t index, const KEY& key) {
    if (!contentionSplitEnabled || currentPage.id == root) {
        return {false, false};
    }
    bool contentionSplitAttempt = false;
    bool contentionSplit = false;
    // perform the detection
    static thread_local std::default_random_engine engine;
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    const double r = distribution(engine);
    const size_t lastUpdate = currentPage.lastUpdatesPos;
    if (r < d1) {
        currentPage.updates++;
        currentPage.slowPaths += !fastPath;
        currentPage.lastUpdatesPos = index;
    }
    if (r < d2) {
        // found contention on two different indexes
        double ratio = currentPage.slowPaths / static_cast<double>(currentPage.updates);
        if (ratio > d3 && lastUpdate != index) {
            auto& parentNode = getNode(parentPage);
            const size_t keyAmount = parentNode.keyAmount;
            if (keyAmount < parentNode.keys.size() - 1) {
                contentionSplitAttempt = true;
                // re-lock
                currentPage.mutex.unlock();
                parentPage.mutex.unlock_shared();
                parentPage.mutex.lock();
                currentPage.mutex.lock();
                // check if the contention still exists
                auto& currentNode = getNode(currentPage);
                const size_t currentIndex = findChildrenIndex(parentNode, key);
                const size_t midIndex = (lastUpdate + index + 1) / 2;
                assert(currentNode.leaf);
                if (midIndex < currentNode.keyAmount &&
                    parentNode.keyAmount > 0 &&
                    parentNode.keyAmount < parentNode.keys.size() &&
                    currentNode.keys[index] == key) {
                    // split
                    uint64_t leftID = split(currentNode, midIndex);
                    // childnode -[split]-> leftNode, childnode
                    currentNode.children[currentNode.keyAmount] = leftID;
                    simpleInsert(parentNode, currentIndex, currentNode.keys[0], leftID);
                    contentionSplit = true;
                    assert(parentNode.keyAmount <= parentNode.keys.size());
#ifdef LOGGING
                    CONTENTION_SPLITS++;
#endif
                }
            }
        }
        // reset the page
        currentPage.lastUpdatesPos = 0;
        currentPage.updates = 0;
        currentPage.slowPaths = 0;
    }
    return {contentionSplitAttempt, contentionSplit};
}

// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
void BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::insert(uint64_t id, KEY key, DATA data) {
    buffer::Page<PAGE_SIZE>* page;
    while (!(page = bufferManager.pinPage(id, true)))
        ;
    // get lock on node
    std::unique_lock lock(page->mutex);
    auto& node = getNode(*page);
    assert(node.keyAmount < node.keys.size());
    size_t index = findChildrenIndex(node, key);
    if (node.leaf) {
        assert(node.keyAmount < node.keys.size());
        // create a new data frame
        auto [newID, frame] = std::move(diskManager.createPage());
        getData(frame) = std::move(data);
        diskManager.writePage(newID, std::move(frame));
        // insert into node
        simpleInsert(node, index, std::move(key), newID);
        // special case: root is leaf + overflow
        if (id == root && node.keyAmount == node.keys.size()) {
            // save key
            KEY midKey = node.keys[node.keyAmount / 2];
            // split
            const uint64_t leftID = split(node, node.keys.size() / 2);
            const uint64_t newRightID = bufferManager.newPage();
            buffer::Page<PAGE_SIZE>* leftPage;
            while (!(leftPage = bufferManager.pinPage(leftID, true)))
                ;
            auto& leftNode = getNode(*leftPage);
            // set pointer
            leftNode.children[leftNode.keyAmount] = newRightID;
            bufferManager.unpinPage(leftID, true);
            // insert
            // pin the new page
            buffer::Page<PAGE_SIZE>* newRightPage;
            while (!(newRightPage = bufferManager.pinPage(newRightID)))
                ;
            initializeNode(*newRightPage);
            auto& newRightNode = getNode(*newRightPage);
            // swap root and the new right node
            std::swap(newRightNode, node);
            bufferManager.unpinPage(newRightID, true);
            // now, insert left and newRight into the root
            node.leaf = false;
            node.keyAmount = 1;
            node.keys[0] = std::move(midKey);
            node.children[0] = leftID;
            node.children[1] = newRightID;
        }
        lock.unlock();
        bufferManager.unpinPage(id, true);
        return;
    }
    uint64_t childID = node.children[index];
    assert(id != childID);
    // find child and insert
    insert(childID, std::move(key), std::move(data));
    // now check for overflow
    buffer::Page<PAGE_SIZE>* childPage;
    while (!(childPage = bufferManager.pinPage(childID, true)))
        ;
    // get lock on child
    std::unique_lock childLock(childPage->mutex);
    auto& childNode = getNode(*childPage);
    // overflow occurred
    if (childNode.keyAmount == childNode.keys.size()) {
        // split
        const uint64_t leftID = split(childNode, childNode.keys.size() / 2);
        // childnode -[split]-> leftNode, childnode
        if (childNode.leaf) {
            // set the pointer
            childNode.children[childNode.keyAmount] = leftID;
            // insert
            assert(node.keyAmount < node.keys.size());
            simpleInsert(node, index, childNode.keys[0], leftID);
        } else {
            buffer::Page<PAGE_SIZE>* leftPage;
            while (!(leftPage = bufferManager.pinPage(leftID, true)))
                ;
            auto& leftNode = getNode(*leftPage);
            leftNode.children[leftNode.keyAmount] = childNode.children[0];
            bufferManager.unpinPage(leftID, true);
            KEY midKey = std::move(childNode.keys[0]);
            // shift right node 1 to the left
            std::move(std::begin(childNode.keys) + 1, std::end(childNode.keys), std::begin(childNode.keys));
            std::move(std::begin(childNode.children) + 1, std::end(childNode.children), std::begin(childNode.children));
            childNode.keyAmount--;
            // insert
            assert(node.keyAmount < node.keys.size());
            simpleInsert(node, index, std::move(midKey), leftID);
        }
    }
    childLock.unlock();
    bufferManager.unpinPage(childID, true);
    // special case: root overflow
    if (id == root && node.keyAmount == node.keys.size()) {
        // split
        const uint64_t leftID = split(node, node.keys.size() / 2);
        buffer::Page<PAGE_SIZE>* leftPage;
        while (!(leftPage = bufferManager.pinPage(leftID, true)))
            ;
        auto& leftNode = getNode(*leftPage);
        leftNode.children[leftNode.keyAmount] = node.children[0];
        bufferManager.unpinPage(leftID, true);
        // save key
        KEY key = std::move(node.keys[0]);
        // shift right node 1 to the left
        std::move(std::begin(node.keys) + 1, std::end(node.keys), std::begin(node.keys));
        std::move(std::begin(node.children) + 1, std::end(node.children), std::begin(node.children));
        node.keyAmount--;
        // create a new page and pin
        const uint64_t newRightID = bufferManager.newPage();
        buffer::Page<PAGE_SIZE>* newRightPage;
        while (!(newRightPage = bufferManager.pinPage(newRightID)))
            ;
        initializeNode(*newRightPage);
        auto& newRightNode = getNode(*newRightPage);
        // swap root and the new right node
        std::swap(newRightNode, node);
        bufferManager.unpinPage(newRightID, true);
        // now, insert left and newRight into the root
        node.leaf = false;
        node.keyAmount = 1;
        node.keys[0] = std::move(key);
        node.children[0] = leftID;
        node.children[1] = newRightID;
    }
    lock.unlock();
    bufferManager.unpinPage(id, true);
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
bool BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::isInnerNode(
    buffer::Page<PAGE_SIZE>* page){
    assert(page);
    return !getNode(*page).leaf;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
bool BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::tryXMerge(
    uint64_t pageID,
    std::unordered_map<uint64_t, size_t>& loadedPages,
    std::unordered_set<uint64_t>& innerNodes,
    std::array<std::unique_ptr<buffer::Page<PAGE_SIZE>>, PAGE_AMOUNT>& buffer,
    disk::DiskManager<PAGE_SIZE>& bufferDiskManager) {
    // the function assumes that the required locks are held and that
    // the requested page is not in memory
    if(innerNodes.empty()){
        return false;
    }
    assert(!loadedPages.contains(pageID));
    static thread_local std::default_random_engine engine;
    std::uniform_int_distribution<> distribution(0, innerNodes.size() - 1);
    auto randomPageIt = innerNodes.begin();
    std::advance(randomPageIt, distribution(engine));
    assert(loadedPages.contains(*randomPageIt));
    const size_t randomIndex = loadedPages.at(*randomPageIt);
    // look for a random page which could work
    auto& ptr = buffer[randomIndex];
    if (!ptr || ptr->deleted || ptr->pinned > 0) {
        return false;
    }
    auto& node = getNode(*ptr);
    if (node.leaf || node.keyAmount <= 1) {
        return false;
    }
    // found one; search for a range of children which are loaded
    // and not currently used
    size_t currentSlots = 0; // amount of free key slots
    constexpr size_t maxMergedNodes = 6;
    distribution = std::uniform_int_distribution<>(0, node.keyAmount - maxMergedNodes);
    const size_t randomStartingIndex = distribution(engine);
    size_t startingIndex = randomStartingIndex;
    std::vector<buffer::Page<PAGE_SIZE>*> currentlyUsed;
    const auto clear = [&](size_t currentIndex) {
        currentSlots = 0;
        startingIndex = currentIndex + 1;
        currentlyUsed.clear();
    };
    for (size_t i = randomStartingIndex; i < node.keyAmount + 1 &&
         i < randomStartingIndex + maxMergedNodes;
         i++) {
        // check if the child is usable
        const uint64_t childID = node.children[i];
        if (!loadedPages.contains(childID)) {
            // child is not loaded
            clear(i);
            continue;
        }
        auto& childPtr = buffer[loadedPages.at(childID)];
        assert(childPtr);
        if (childPtr->deleted) {
            // child was deleted
            clear(i);
            continue;
        }
        if (childPtr->pinned > 0) {
            // child is currently being used
            clear(i);
            continue;
        }
        auto& childNode = getNode(*childPtr);
        // get the free slots of the child
        const size_t freeSlots = childNode.keys.size() - childNode.keyAmount - 1;
        currentSlots += freeSlots;
        currentlyUsed.push_back(childPtr.get());
        // check if the current combination would be enough
        if (childNode.leaf && currentSlots < node.keys.size()) {
            continue;
        }
        if (!childNode.leaf && currentSlots < node.keys.size() + 1) {
            continue;
        }
        // perform the merge
        size_t rightChildIndex = currentlyUsed.size() - 1;
        size_t leftChildIndex = rightChildIndex - 1;
        if (childNode.leaf) {
            // go from right to left
            while (leftChildIndex != static_cast<size_t>(-1)) {
                size_t currentKeyIndex = startingIndex + rightChildIndex - 1;
                auto* leftPage = currentlyUsed[leftChildIndex];
                auto* rightPage = currentlyUsed[rightChildIndex];
                assert(leftPage->pinned == 0);
                assert(rightPage->pinned == 0);
                auto& leftNode = getNode(*leftPage);
                auto& rightNode = getNode(*rightPage);
                assert(leftNode.leaf);
                assert(rightNode.leaf);
                leftPage->modified = true;
                rightPage->modified = true;
                const size_t slotsToBeMoved = std::min(
                    leftNode.keyAmount,
                    static_cast<uint32_t>(rightNode.keys.size()) - rightNode.keyAmount - 1);
                // make room in the right node
                std::move(rightNode.keys.begin(),
                          rightNode.keys.begin() + rightNode.keyAmount,
                          rightNode.keys.begin() + slotsToBeMoved);
                std::move(rightNode.children.begin(),
                          rightNode.children.begin() + rightNode.keyAmount,
                          rightNode.children.begin() + slotsToBeMoved);
                // move from left to right
                std::move(leftNode.keys.begin() + leftNode.keyAmount - slotsToBeMoved,
                          leftNode.keys.begin() + leftNode.keyAmount,
                          rightNode.keys.begin());
                std::move(leftNode.children.begin() + leftNode.keyAmount - slotsToBeMoved,
                          leftNode.children.begin() + leftNode.keyAmount,
                          rightNode.children.begin());
                // set key amount
                leftNode.children[leftNode.keyAmount] = rightPage->id;
                leftNode.keyAmount -= slotsToBeMoved;
                rightNode.keyAmount += slotsToBeMoved;
                // move right pointer
                bool movedLeft = false;
                if (rightNode.keyAmount == rightNode.keys.size() - 1) {
                    rightChildIndex--;
                    if (leftChildIndex == rightChildIndex) {
                        leftChildIndex--;
                        movedLeft = true;
                    }
                    // copy the key to the parent
                    node.keys[currentKeyIndex] = rightNode.keys[0];
                }
                if (!movedLeft && leftNode.keyAmount == 0) {
                    // move left pointer
                    leftChildIndex--;
                }
            }
        } else {
            // go from right to left
            while (leftChildIndex != static_cast<size_t>(-1)) {
                const size_t currentKeyIndex = startingIndex + rightChildIndex - 1;
                auto* leftPage = currentlyUsed[leftChildIndex];
                auto* rightPage = currentlyUsed[rightChildIndex];
                assert(leftPage->pinned == 0);
                assert(rightPage->pinned == 0);
                auto& leftNode = getNode(*leftPage);
                auto& rightNode = getNode(*rightPage);
                assert(!leftNode.leaf);
                assert(!rightNode.leaf);
                leftPage->modified = true;
                rightPage->modified = true;
                const size_t slotsToBeMoved = std::min(
                    leftNode.keyAmount,
                    static_cast<uint32_t>(rightNode.keys.size()) - rightNode.keyAmount - 1);
                // make room in the right node
                std::move(rightNode.keys.begin(),
                          rightNode.keys.begin() + rightNode.keyAmount,
                          rightNode.keys.begin() + slotsToBeMoved);
                std::move(rightNode.children.begin(),
                          rightNode.children.begin() + rightNode.keyAmount + 1,
                          rightNode.children.begin() + slotsToBeMoved);
                const bool mustTransferChild = rightChildIndex - leftChildIndex == 2;
                assert(rightChildIndex - leftChildIndex <= 2);
                size_t directlyMoved;
                if (mustTransferChild) {
                    directlyMoved = slotsToBeMoved >= 2 ? slotsToBeMoved - 2 : 0;
                } else {
                    directlyMoved = slotsToBeMoved >= 1 ? slotsToBeMoved - 1 : 0;
                }
                assert(directlyMoved <= slotsToBeMoved);
                // move the parent key(s)
                for (size_t i = 0; i < slotsToBeMoved - directlyMoved; i++) {
                    rightNode.keys[slotsToBeMoved - 1 - i] = std::move(node.keys[currentKeyIndex - i]);
                }
                if (mustTransferChild) {
                    rightNode.children[slotsToBeMoved - 1] =
                        getNode(*currentlyUsed[rightChildIndex - 1]).children[0];
                }
                // move from left to right
                std::move(leftNode.keys.begin() + leftNode.keyAmount - directlyMoved,
                          leftNode.keys.begin() + leftNode.keyAmount,
                          rightNode.keys.begin());
                std::move(leftNode.children.begin() + leftNode.keyAmount - slotsToBeMoved + 1 + mustTransferChild,
                          leftNode.children.begin() + leftNode.keyAmount + 1,
                          rightNode.children.begin());
                if (mustTransferChild) {
                    // move the last child into the empty node
                    getNode(*currentlyUsed[rightChildIndex - 1]).children[0] =
                        leftNode.children[leftNode.keyAmount + 1 - slotsToBeMoved];
                }
                // move the leftmost child(s) of the left node into the parent
                for (size_t i = 0; i < slotsToBeMoved - directlyMoved; i++) {
                    node.keys[currentKeyIndex - i] =
                        std::move(leftNode.keys[leftNode.keyAmount - directlyMoved - 1 - i]);
                }
                if (slotsToBeMoved - directlyMoved == 1 && mustTransferChild) {
                    std::swap(node.keys[currentKeyIndex], node.keys[currentKeyIndex - 1]);
                }
                // set key amount
                leftNode.keyAmount -= slotsToBeMoved;
                rightNode.keyAmount += slotsToBeMoved;
                if (leftChildIndex == 0 && rightChildIndex == 1) {
                    // last node; transfer the parent key and leftmost child as well
                    // make room in the right node
                    std::move(rightNode.keys.begin(),
                              rightNode.keys.begin() + rightNode.keyAmount,
                              rightNode.keys.begin() + 1);
                    std::move(rightNode.children.begin(),
                              rightNode.children.begin() + rightNode.keyAmount + 1,
                              rightNode.children.begin() + 1);
                    // transfer
                    rightNode.keys[0] = std::move(node.keys[currentKeyIndex]);
                    rightNode.children[0] = std::move(leftNode.children[0]);
                    rightNode.keyAmount++;
                    assert(rightNode.keyAmount < rightNode.keys.size());
                }
                bool movedLeft = false;
                if (rightNode.keyAmount == rightNode.keys.size() - 1) {
                    rightChildIndex--;
                    if (leftChildIndex == rightChildIndex) {
                        leftChildIndex--;
                        movedLeft = true;
                    }
                }
                if (!movedLeft && leftNode.keyAmount == 0) {
                    // move left pointer
                    leftChildIndex--;
                }
            }
        }
        // now the leftmost node is free; adjust the parent
        std::move(node.keys.begin() + startingIndex + 1,
                  node.keys.end(),
                  node.keys.begin() + startingIndex);
        std::move(node.children.begin() + startingIndex + 1,
                  node.children.end(),
                  node.children.begin() + startingIndex);
        node.keyAmount--;
        // mark the node as modified
        ptr->modified = true;
        assert(node.keyAmount >= 1);
        const size_t firstID = currentlyUsed[0]->id;
        // the first node was freed; now we can use its place in the buffer
        // delete the first node
        const size_t firstPageHand = loadedPages.at(firstID);
        bufferDiskManager.deletePage(firstID);
        loadedPages.erase(firstID);
        innerNodes.erase(firstID);
        // load the new page
        auto newPage = std::make_unique<buffer::Page<PAGE_SIZE>>(
            pageID, std::move(bufferDiskManager.retrievePage(pageID)));
        buffer[firstPageHand] = std::move(newPage);
        loadedPages[pageID] = firstPageHand;
        return true;
    }
    // unsuccessful
    return false;
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
size_t BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::size() const {
    return diskManager.entryAmount();
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
std::optional<DATA> BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::find(const KEY& key) {
    uint64_t parentID = root;
    buffer::Page<PAGE_SIZE>* parentPage;
    while (!(parentPage = bufferManager.pinPage(parentID, true)))
        ;
    parentPage->mutex.lock_shared();
    while (true) {
        assert(parentPage->pinned > 0);
        auto& parentNode = getNode(*parentPage);
        if (parentNode.leaf) {
            for (size_t i = 0; i < parentNode.keyAmount; i++) {
                if (parentNode.keys[i] == key) {
                    uint64_t id = parentNode.children[i];
                    auto frame = std::move(diskManager.retrievePage(id));
                    DATA data = std::move(getData(frame));
                    parentPage->mutex.unlock_shared();
                    bufferManager.unpinPage(parentID, false);
                    return data;
                }
            }
            parentPage->mutex.unlock_shared();
            bufferManager.unpinPage(parentID, false);
            return std::nullopt;
        }
        uint64_t currentID = parentNode.children[findChildrenIndex(parentNode, key)];
        // pin page
        buffer::Page<PAGE_SIZE>* currentPage;
        while (!(currentPage = bufferManager.pinPage(currentID, true)))
            ;
        currentPage->mutex.lock_shared();
        parentPage->mutex.unlock_shared();
        bufferManager.unpinPage(parentID, false);
        // set for next round
        parentID = currentID;
        parentPage = currentPage;
    }
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
void BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::insert(KEY key, DATA data) {
    insert(root, std::move(key), std::move(data));
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
bool BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::contains(const KEY& key) {
    uint64_t parentID = root;
    buffer::Page<PAGE_SIZE>* parentPage;
    while (!(parentPage = bufferManager.pinPage(parentID, true)))
        ;
    parentPage->mutex.lock_shared();
    while (true) {
        assert(parentPage->pinned > 0);
        auto& parentNode = getNode(*parentPage);
        for (size_t i = 0; i < parentNode.keyAmount; i++) {
            if (parentNode.keys[i] == key) {
                parentPage->mutex.unlock_shared();
                bufferManager.unpinPage(parentID, false);
                return true;
            }
        }
        if (parentNode.leaf) {
            parentPage->mutex.unlock_shared();
            bufferManager.unpinPage(parentID, false);
            return false;
        }
        uint64_t currentID = parentNode.children[findChildrenIndex(parentNode, key)];
        // pin page
        buffer::Page<PAGE_SIZE>* currentPage;
        while (!(currentPage = bufferManager.pinPage(currentID, true)))
            ;
        currentPage->mutex.lock_shared();
        parentPage->mutex.unlock_shared();
        bufferManager.unpinPage(parentID, false);
        // set for next round
        parentID = currentID;
        parentPage = currentPage;
    }
}
// --------------------------------------------------------------------------
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
bool BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::update(
    const KEY& key, const std::function<void(DATA&)>& func) {
    buffer::Page<PAGE_SIZE>* parentPage = nullptr;
    buffer::Page<PAGE_SIZE>* currentPage;
    while (!(currentPage = bufferManager.pinPage(root, true)))
        ;
    currentPage->mutex.lock_shared();
    while (true) {
        auto& currentNode = getNode(*currentPage);
        assert(currentPage->pinned > 0);
        if (currentNode.leaf) {
            // -> we need to lock it exclusively
            currentPage->mutex.unlock_shared();
            bool fastPath = currentPage->mutex.try_lock();
            if (!fastPath) {
                currentPage->mutex.lock();
            }
            if(currentNode.leaf){
                // now current is exclusively held (the parent is shared)
                for (size_t i = 0; i < currentNode.keyAmount; i++) {
                    if (currentNode.keys[i] == key) {
                        uint64_t id = currentNode.children[i];
                        auto frame = std::move(diskManager.retrievePage(id));
                        func(getData(frame));
                        diskManager.writePage(id, frame);
                        // CONTENTION SPLIT
                        bool contentionSplitAttempt = false;
                        bool contentionSplit = false;
                        if (currentPage->id != root && parentPage->id != root) {
                            assert(parentPage != nullptr);
                            assert(currentPage->pinned > 0);
                            assert(parentPage->pinned > 0);
                            auto result = tryContentionSplit(*parentPage, *currentPage, fastPath, i, key);
                            contentionSplitAttempt = result.first;
                            contentionSplit = result.second;
                        }
                        if (contentionSplitAttempt) {
                            parentPage->mutex.unlock();
                        } else if (parentPage) {
                            parentPage->mutex.unlock_shared();
                        }
                        currentPage->mutex.unlock();
                        if (parentPage) {
                            assert(parentPage->pinned >= 1);
                            bufferManager.unpinPage(parentPage->id, contentionSplit);
                        }
                        assert(currentPage->pinned >= 1);
                        bufferManager.unpinPage(currentPage->id, contentionSplit);
                        return true;
                    }
                }
                if (parentPage) {
                    parentPage->mutex.unlock_shared();
                }
                currentPage->mutex.unlock();
                if (parentPage) {
                    assert(parentPage->pinned >= 1);
                    bufferManager.unpinPage(parentPage->id, false);
                }
                assert(currentPage->pinned >= 1);
                bufferManager.unpinPage(currentPage->id, false);
                return false;
            }
            currentPage->mutex.unlock();
            currentPage->mutex.lock_shared();
        }
        const uint64_t nextID = currentNode.children[findChildrenIndex(currentNode, key)];
        // pin page
        buffer::Page<PAGE_SIZE>* nextPage;
        while (!(nextPage = bufferManager.pinPage(nextID, true)))
            ;
        nextPage->mutex.lock_shared();
        if (parentPage) {
            parentPage->mutex.unlock_shared();
            assert(parentPage->pinned >= 1);
            bufferManager.unpinPage(parentPage->id, false);
        }
        // set for next round
        parentPage = currentPage;
        currentPage = nextPage;
    }
}
// --------------------------------------------------------------------------
/*
template <class KEY, class DATA, size_t PAGE_AMOUNT, size_t TOTAL_PAGE_SIZE>
requires ValidPageSize<KEY, TOTAL_PAGE_SIZE>
void BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE>::print(uint64_t id, bool first) {
    buffer::Page<PAGE_SIZE>* page;
    while (!(page = bufferManager.pinPage(id, true)))
        ;
    auto& node = getNode(*page);
    bool leaf = node.leaf;
    if (first) {
        std::cout << "digraph{\n";
    }
    std::cout << id << "[label=\"";
    for (size_t i = 0; i < node.keyAmount; i++) {
        std::cout << static_cast<bool>(node.keys[i]) << " ";
    }
    std::cout << "\"];\n";
    std::vector<uint64_t> children;
    for (size_t i = 0; i <= node.keyAmount; i++) {
        children.push_back(node.children[i]);
    }
    bufferManager.unpinPage(id, false);
    // print children
    if (!leaf) {
        for (uint64_t child : children) {
            print(child, false);
            std::cout << id << " -> " << child << ";\n";
        }
    } else {
        // remove pointer
        children.pop_back();
        std::cout << "d" << id << "[label=\"";
        for (uint64_t child : children) {
            std::cout << "[" << child << "-> (data of" << child << ")" << "] ";
        }
        std::cout << "\"];\n";
        std::cout << id << " -> d" << id << ";\n";
    }

    if (first) {
        std::cout << "}" << std::endl;
    }
}
 */
// --------------------------------------------------------------------------
} // namespace btree
// --------------------------------------------------------------------------
#endif //BTREE_BTREE_H
