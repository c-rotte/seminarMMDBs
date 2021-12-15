#include <gtest/gtest.h>
// --------------------------------------------------------------------------
#include "src/buffer/DiskManager.h"
#include <chrono>
#include <thread>
// --------------------------------------------------------------------------
using namespace std;
using namespace disk;
// --------------------------------------------------------------------------
namespace {
// --------------------------------------------------------------------------
static const string FILENAME = "/tmp/data.txt";
constexpr size_t BLOCK_SIZE = 4096;
void setup() {
    std::remove(FILENAME.c_str());
}
// --------------------------------------------------------------------------
} // namespace
// --------------------------------------------------------------------------
TEST(DiskManager, StoreData) {
    setup();
    DiskManager<BLOCK_SIZE> manager(FILENAME);
    for (size_t i = 0; i < 1000; i++) {
        auto p = manager.createPage();
        EXPECT_EQ(i, p.first);
        std::memset(p.second.content.data(), static_cast<char>(i % 100), BLOCK_SIZE);
        manager.writePage(p.first, std::move(p.second));
        EXPECT_EQ(manager.entryAmount(), i + 1);
    }
    for (size_t i = 0; i < 1000; i++) {
        Frame<BLOCK_SIZE> frame = manager.retrievePage(i);
        for (char c : frame.content) {
            EXPECT_EQ(c, i % 100);
        }
    }
}
// --------------------------------------------------------------------------
TEST(DiskManager, DeleteData) {
    setup();
    DiskManager<BLOCK_SIZE> manager(FILENAME);
    for (size_t i = 0; i < 1000; i++) {
        auto p = manager.createPage();
        std::memset(p.second.content.data(), static_cast<char>(p.first % 100), BLOCK_SIZE);
        manager.writePage(p.first, std::move(p.second));
        if (i % 2 == 0) {
            size_t entryAmount = manager.entryAmount();
            manager.deletePage(p.first);
            EXPECT_EQ(manager.entryAmount(), entryAmount - 1);
        }
    }
    for (size_t i = 0; i < 500; i++) {
        Frame<BLOCK_SIZE> frame = manager.retrievePage(i);
        for (char c : frame.content) {
            EXPECT_EQ(c, i % 100);
        }
    }
    EXPECT_EQ(std::filesystem::file_size(FILENAME),
              sizeof(Header) + 500 * sizeof(Frame<BLOCK_SIZE>));
}
// --------------------------------------------------------------------------
TEST(DiskManager, RestoreData) {
    setup();
    {
        DiskManager<BLOCK_SIZE> manager(FILENAME);
        for (size_t i = 0; i < 1000; i++) {
            auto p = manager.createPage();
            std::memset(p.second.content.data(), static_cast<char>(p.first % 100), BLOCK_SIZE);
            manager.writePage(p.first, std::move(p.second));
            if (i % 2 == 0) {
                manager.deletePage(p.first);
            }
        }
        for (size_t i = 0; i < 500; i++) {
            Frame<BLOCK_SIZE> frame = manager.retrievePage(i);
            for (char c : frame.content) {
                EXPECT_EQ(c, i % 100);
            }
        }
        EXPECT_EQ(std::filesystem::file_size(FILENAME),
                  sizeof(Header) + 500 * sizeof(Frame<BLOCK_SIZE>));
    }
    // reopen file
    DiskManager<BLOCK_SIZE> manager(FILENAME);
    for (size_t i = 0; i < 500; i++) {
        Frame<BLOCK_SIZE> frame = manager.retrievePage(i);
        for (char c : frame.content) {
            EXPECT_EQ(c, i % 100);
        }
    }
    EXPECT_EQ(std::filesystem::file_size(FILENAME),
              sizeof(Header) + 500 * sizeof(Frame<BLOCK_SIZE>));
}
// --------------------------------------------------------------------------
TEST(DiskManager, DeleteAllData) {
    setup();
    DiskManager<BLOCK_SIZE> manager(FILENAME);
    for (size_t i = 0; i < 1000; i++) {
        auto p = manager.createPage();
        std::memset(p.second.content.data(), static_cast<char>(p.first % 100), BLOCK_SIZE);
        manager.writePage(p.first, std::move(p.second));
    }
    for (size_t i = 1000; i > 0; i--) {
        manager.deletePage(i - 1);
    }
    auto p = manager.createPage();
    EXPECT_EQ(p.first, 0);
    EXPECT_EQ(std::filesystem::file_size(FILENAME),
              sizeof(Header) + 1000 * sizeof(Frame<BLOCK_SIZE>));
}
// --------------------------------------------------------------------------
TEST(DiskManager, MultiThreadedAccess) {
    setup();
    DiskManager<BLOCK_SIZE> manager(FILENAME);
    // start multiple threads
    std::vector<std::thread> threads;
    for (size_t i = 0; i < 1000; i++) {
        threads.emplace_back([&manager]() {
            for (size_t i = 0; i < 100; i++) {
                auto p = manager.createPage();
                std::memset(p.second.content.data(), static_cast<char>(p.first % 100), BLOCK_SIZE);
                manager.writePage(p.first, std::move(p.second));
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 10));
                auto frame = manager.retrievePage(p.first);
                for (char c : frame.content) {
                    EXPECT_EQ(c, p.first % 100);
                }
                manager.deletePage(p.first);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
}