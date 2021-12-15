#ifndef BTREE_DISKMANAGER_H
#define BTREE_DISKMANAGER_H
// --------------------------------------------------------------------------
#include <array>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <fcntl.h>
#include <unistd.h>
// --------------------------------------------------------------------------
namespace disk {
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
struct __attribute__((packed)) Frame {
    // actual data
    alignas(max_align_t) std::array<char, BLOCK_SIZE> content = {};
    // determines whether the Frame is used (1) or free (0)
    bool used = false;
    // if the frame is free, it contains a pointer (uint64_t) to the next
    // free frame
    uint64_t nextFreeFrame = 0;
};
// --------------------------------------------------------------------------
struct __attribute__((packed)) Header {
    uint64_t blockSize = 0;
    uint64_t totalBlocks = 0;
    uint64_t freeBlocks = 0;
    uint64_t freeListHeader = 0;
};
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
class DiskManager {
    private:
    int fd;
    Header header;
    mutable std::shared_mutex mutex;

    public:
    explicit DiskManager(const std::string&);
    ~DiskManager();

    private:
    void flushHeader();
    bool isUsed(uint64_t);
    void setUsed(uint64_t, bool);
    void setNext(uint64_t, uint64_t);
    Frame<BLOCK_SIZE> retrievePageHelper(uint64_t);

    public:
    size_t entryAmount() const;
    std::pair<uint64_t, Frame<BLOCK_SIZE>> createPage();
    void deletePage(uint64_t);
    Frame<BLOCK_SIZE> retrievePage(uint64_t);
    void writePage(uint64_t, const Frame<BLOCK_SIZE>&);
};
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
DiskManager<BLOCK_SIZE>::DiskManager(const std::string& path) {
    if (std::filesystem::exists(path) && std::filesystem::is_regular_file(path)) {
        // open file
        fd = open(path.c_str(), O_RDWR, S_IRWXU);
        // read header
        if (pread(fd, &header, sizeof(Header), 0) != sizeof(Header)) {
            throw std::runtime_error("invalid file");
        }
        if (header.blockSize != BLOCK_SIZE) {
            throw std::runtime_error("different block sizes");
        }
    } else {
        // create file
        fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
        // create header
        header = {
            BLOCK_SIZE,
            0, // allocate 0 blocks
            0, // no free blocks
            0};
        Frame<BLOCK_SIZE> frame;
        // write header
        flushHeader();
    }
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
DiskManager<BLOCK_SIZE>::~DiskManager() {
    flushHeader();
    close(fd);
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
void DiskManager<BLOCK_SIZE>::flushHeader() {
    if (pwrite(fd, &header, sizeof(Header), 0) != sizeof(Header)) {
        throw std::runtime_error("couldn't write header");
    }
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
bool DiskManager<BLOCK_SIZE>::isUsed(uint64_t id) {
    char buff;
    if (pread(fd, &buff, sizeof(Frame<BLOCK_SIZE>::used),
              sizeof(Header) + id * sizeof(Frame<BLOCK_SIZE>) +
                  offsetof(Frame<BLOCK_SIZE>, used)) != sizeof(Frame<BLOCK_SIZE>::used)) {
        throw std::runtime_error("couldn't read flag");
    }
    return buff;
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
void DiskManager<BLOCK_SIZE>::setUsed(uint64_t id, bool used) {
    if (pwrite(fd, &used, sizeof(Frame<BLOCK_SIZE>::used),
               sizeof(Header) + id * sizeof(Frame<BLOCK_SIZE>) +
                   offsetof(Frame<BLOCK_SIZE>, used)) != sizeof(Frame<BLOCK_SIZE>::used)) {
        throw std::runtime_error("couldn't write flag");
    }
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
void DiskManager<BLOCK_SIZE>::setNext(uint64_t id, uint64_t next) {
    if (pwrite(fd, &next, sizeof(Frame<BLOCK_SIZE>::nextFreeFrame),
               sizeof(Header) + id * sizeof(Frame<BLOCK_SIZE>) +
                   offsetof(Frame<BLOCK_SIZE>, nextFreeFrame)) != sizeof(Frame<BLOCK_SIZE>::nextFreeFrame)) {
        throw std::runtime_error("couldn't write next");
    }
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
Frame<BLOCK_SIZE> DiskManager<BLOCK_SIZE>::retrievePageHelper(uint64_t id) {
    Frame<BLOCK_SIZE> frame;
    if (pread(fd, &frame, sizeof(Frame<BLOCK_SIZE>),
              sizeof(Header) + id * sizeof(Frame<BLOCK_SIZE>)) != sizeof(Frame<BLOCK_SIZE>)) {
        throw std::runtime_error("couldn't get page");
    }
    return frame;
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
size_t DiskManager<BLOCK_SIZE>::entryAmount() const {
    std::unique_lock lock(mutex);
    return header.totalBlocks - header.freeBlocks;
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
std::pair<uint64_t, Frame<BLOCK_SIZE>> DiskManager<BLOCK_SIZE>::createPage() {
    std::unique_lock lock(mutex);
    // check if enough pages are free
    if (header.freeBlocks > 0) {
        // the header points to a free frame; use that
        uint64_t freePage = header.freeListHeader;
        // mark the frame as used
        setUsed(freePage, true);
        Frame<BLOCK_SIZE> frame = retrievePageHelper(freePage);
        // copy the id the free frame points to
        header.freeListHeader = frame.nextFreeFrame;
        header.freeBlocks--;
        flushHeader();
        return std::make_pair(freePage, std::move(frame));
    } else {
        // create a new frame with an id
        uint64_t newID = header.totalBlocks;
        Frame<BLOCK_SIZE> newFrame;
        newFrame.used = true;
        // write it to the end of the file
        if (pwrite(fd, &newFrame, sizeof(Frame<BLOCK_SIZE>),
                   sizeof(Header) + header.totalBlocks * sizeof(Frame<BLOCK_SIZE>)) != sizeof(Frame<BLOCK_SIZE>)) {
            throw std::runtime_error("couldn't write frame");
        }
        header.totalBlocks++;
        flushHeader();
        return std::make_pair(newID, std::move(newFrame));
    }
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
void DiskManager<BLOCK_SIZE>::deletePage(uint64_t id) {
    if (!isUsed(id)) {
        return;
    }
    setUsed(id, false);
    std::unique_lock lock(mutex);
    setNext(id, header.freeListHeader);
    header.freeListHeader = id;
    header.freeBlocks++;
    flushHeader();
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
Frame<BLOCK_SIZE> DiskManager<BLOCK_SIZE>::retrievePage(uint64_t id) {
    return retrievePageHelper(id);
}
// --------------------------------------------------------------------------
template <size_t BLOCK_SIZE>
void DiskManager<BLOCK_SIZE>::writePage(uint64_t id, const Frame<BLOCK_SIZE>& frame) {
    if (pwrite(fd, &frame, sizeof(Frame<BLOCK_SIZE>),
               sizeof(Header) + id * sizeof(Frame<BLOCK_SIZE>)) != sizeof(Frame<BLOCK_SIZE>)) {
        throw std::runtime_error("couldn't write frame");
    }
    return;
}
// --------------------------------------------------------------------------
} // namespace disk
// --------------------------------------------------------------------------
#endif //BTREE_DISKMANAGER_H
