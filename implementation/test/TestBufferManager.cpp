#include <gtest/gtest.h>
// --------------------------------------------------------------------------
#include "src/buffer/BufferManager.h"
#include <chrono>
#include <thread>
#include <unordered_set>
// --------------------------------------------------------------------------
using namespace std;
using namespace buffer;
// --------------------------------------------------------------------------
namespace {
// --------------------------------------------------------------------------
static const string FILENAME = "/tmp/data.txt";
constexpr size_t PAGE_AMOUNT = 500;
constexpr size_t PAGE_SIZE = 4096;
void setup() {
    std::remove(FILENAME.c_str());
}
// --------------------------------------------------------------------------
} // namespace
// --------------------------------------------------------------------------
TEST(BufferManager, StoreData) {
    setup();
    BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager(FILENAME);
    unordered_set<uint64_t> ids;

    for (size_t i = 0; i < 2000; i++) {
        ids.insert(bufferManager.newPage());
    }
    for (uint64_t id : ids) {
        auto* page = bufferManager.pinPage(id);
        page->frame.content[0] = id % 100;
        bufferManager.unpinPage(id, true);
    }
    for (int i = 0; i < 50; i++) {
        for (uint64_t id : ids) {
            auto* page = bufferManager.pinPage(id);
            EXPECT_EQ(page->frame.content[0], id % 100);
            bufferManager.unpinPage(id, false);
        }
    }
}
// --------------------------------------------------------------------------
TEST(BufferManager, StoreMultithreaded) {
    setup();
    BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager(FILENAME);
    std::vector<std::thread> threads;

    for (size_t i = 0; i < 10000; i++) {
        threads.emplace_back([&bufferManager, i]() {
            // busy wait if the page could not be loaded
            uint64_t id = bufferManager.newPage();
            while (true) {
                auto* page = bufferManager.pinPage(id);
                if (!page) {
                    continue;
                }
                EXPECT_GE(page->pinned, 0);
                EXPECT_EQ(page->id, id);
                page->frame.content[0] = i % 100;
                bufferManager.unpinPage(id, true);
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 10));
                page = bufferManager.pinPage(id);
                if (!page) {
                    continue;
                }
                EXPECT_EQ(page->frame.content[0], i % 100);
                bufferManager.unpinPage(id, false);
                while (!bufferManager.deletePage(id))
                    ;
                break;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}
// --------------------------------------------------------------------------
TEST(BufferManager, StoreMultithreadedSamePage) {
    setup();
    BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager(FILENAME);
    std::vector<std::thread> threads;

    const uint64_t id = bufferManager.newPage();
    for (size_t i = 0; i < 10000; i++) {
        threads.emplace_back([&bufferManager, i, id]() {
            // busy wait if the page could not be loaded
            while (true) {
                auto* page = bufferManager.pinPage(id);
                if (!page) {
                    continue;
                }
                EXPECT_GE(page->pinned, 0);
                page->frame.content[0] = i % 100;
                bufferManager.unpinPage(id, true);
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 10));
                page = bufferManager.pinPage(id);
                if (!page) {
                    continue;
                }
                bufferManager.unpinPage(id, false);
                break;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}
// --------------------------------------------------------------------------
TEST(BufferManager, StoreMultipleMultithreaded) {
    setup();
    BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager(FILENAME);
    std::vector<std::thread> threads;

    for (size_t i = 0; i < 2500; i++) {
        threads.emplace_back([&bufferManager, i]() {
            // busy wait if the page could not be loaded
            while (true) {
                uint64_t id = bufferManager.newPage();
                for (size_t i = 1; i <= 5000; i++) {
                    const char random = rand() % 100;
                    auto* page = bufferManager.pinPage(id);
                    if (!page) {
                        i--;
                        continue;
                    }
                    page->frame.content[0] = random;
                    bufferManager.unpinPage(id, true);
                    page = bufferManager.pinPage(id);
                    if (!page) {
                        i--;
                        continue;
                    }
                    EXPECT_EQ(page->frame.content[0], random);
                    bufferManager.unpinPage(id, false);
                }
                while (!bufferManager.deletePage(id))
                    ;
                break;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
}
// --------------------------------------------------------------------------
TEST(BufferManager, CheckSize_1) {
    setup();
    {
        BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager(FILENAME);
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 1000; i++) {
            threads.emplace_back([&bufferManager, i]() {
                uint64_t id = bufferManager.newPage();
                // busy wait if the page could not be loaded
                while (true) {
                    const char random = rand() % 100;
                    auto* page = bufferManager.pinPage(id);
                    if (!page) {
                        continue;
                    }
                    page->frame.content[0] = random;
                    bufferManager.unpinPage(id, true);
                    page = bufferManager.pinPage(id);
                    if (!page) {
                        continue;
                    }
                    EXPECT_EQ(page->frame.content[0], random);
                    bufferManager.unpinPage(id, false);
                    break;
                }
            });
        }
        for (auto& t : threads) {
            t.join();
        }
        // the destructor of the buffer manager writes all pages to memory
    }
    EXPECT_EQ(std::filesystem::file_size(FILENAME),
              sizeof(disk::Header) + 1000 * sizeof(disk::Frame<PAGE_SIZE>));
}
// --------------------------------------------------------------------------
TEST(BufferManager, CheckSize_2) {
    setup();
    BufferManager<PAGE_AMOUNT, PAGE_SIZE> bufferManager(FILENAME);
    {
        std::unordered_set<uint64_t> ids;
        // fill the buffer
        for (size_t i = 0; i < PAGE_AMOUNT; i++) {
            uint64_t id = bufferManager.newPage();
            ids.insert(id);
            auto* page = bufferManager.pinPage(id);
            EXPECT_NE(page, nullptr);
            bufferManager.unpinPage(id, rand() % 2);
        }
        // delete all pages
        for (uint64_t id : ids) {
            bufferManager.deletePage(id);
        }
        // fill the buffer again (should be reused)
        for (size_t i = 0; i < PAGE_AMOUNT + 1; i++) {
            uint64_t id = bufferManager.newPage();
            auto* page = bufferManager.pinPage(id);
            EXPECT_NE(page, nullptr);
            bufferManager.unpinPage(id, rand() % 2);
        }
    }
    // total size should be PAGE_AMOUNT + 1 because all pages except the first one were deleted and thus reused
    EXPECT_EQ(std::filesystem::file_size(FILENAME),
              sizeof(disk::Header) + (PAGE_AMOUNT + 1) * sizeof(disk::Frame<PAGE_SIZE>));
}