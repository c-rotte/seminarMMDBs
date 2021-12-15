#include <gtest/gtest.h>
// --------------------------------------------------------------------------
#include "src/btree/BTree.h"
#include <chrono>
#include <thread>
#include <unordered_set>
#include <algorithm>
#include <random>
#include <fstream>
// --------------------------------------------------------------------------
using namespace std;
using namespace btree;
// --------------------------------------------------------------------------
namespace {
// --------------------------------------------------------------------------
static const string BTREE_FILENAME = "/tmp/btree.txt";
static const string DATA_FILENAME = "/tmp/data.txt";
constexpr size_t PAGE_AMOUNT = 500;
constexpr size_t TOTAL_PAGE_SIZE = 256;
using KEY = uint32_t;
using DATA = uint64_t;
// --------------------------------------------------------------------------
void setup() {
    std::remove(BTREE_FILENAME.c_str());
    std::remove(DATA_FILENAME.c_str());
}
// --------------------------------------------------------------------------
array<char, 128> generateRandomString(){
    array<char, 128> arr;
    for(size_t i = 0; i < 128; i++){
        arr[i] = 'a' + rand() % 26;
    }
    return arr;
}
// --------------------------------------------------------------------------
} // namespace
// --------------------------------------------------------------------------
TEST(BTree, StoreData_1) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    for(uint32_t i = 0; i < 1000; i += 2){
        tree.insert(i, i * 2);
    }
    for(uint32_t i = 0; i < 1000; i += 2){
        EXPECT_TRUE(tree.contains(i));
        EXPECT_FALSE(tree.contains(i + 1));
    }
    for(uint32_t i = 0; i < 1000; i += 2){
        auto data = std::move(tree.find(i));
        EXPECT_TRUE(data);
        EXPECT_EQ(*data, i * 2);
        data = std::move(tree.find(i + 1));
        EXPECT_FALSE(data);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, StoreData_2) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    for(int i = 1000; i >= 0; i -= 2){
        tree.insert(i, i * 2);
    }
    for(int i = 1000; i >= 0; i -= 2){
        EXPECT_TRUE(tree.contains(i));
        EXPECT_FALSE(tree.contains(i + 1));
    }
    for(int i = 1000; i >= 0; i -= 2){
        auto data = std::move(tree.find(i));
        EXPECT_TRUE(data);
        EXPECT_EQ(*data, i * 2);
        data = std::move(tree.find(i + 1));
        EXPECT_FALSE(data);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, StoreDataRandom) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    std::vector<KEY> keys;
    for(KEY key = 0; key < 1000; key++){
        keys.push_back(key);
    }
    auto rng = std::default_random_engine();
    std::shuffle(keys.begin(), keys.end(), rng);
    for(KEY key : keys){
        tree.insert(key, key * 2);
    }
    for(KEY key : keys){
        EXPECT_TRUE(tree.contains(key));
    }
    for(KEY key : keys){
        auto data = std::move(tree.find(key));
        EXPECT_TRUE(data);
        EXPECT_EQ(*data, key * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, StoreDataMultiThreaded) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<thread> threads;
    for(size_t i = 0; i < 100 * 1000; i += 1000){
        threads.emplace_back([&tree, i](){
            for(uint32_t key = i; key < i + 1000; key++){
                EXPECT_FALSE(tree.contains(key));
                tree.insert(key, key * 2);
                EXPECT_TRUE(tree.contains(key));
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 100 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        EXPECT_EQ(*data, i * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, StoreDataMultiThreadedLarge) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<thread> threads;
    for(size_t i = 0; i < 100 * 10000; i += 10000){
        threads.emplace_back([&tree, i](){
            for(uint32_t key = i; key < i + 10000; key++){
                tree.insert(key, key * 2);
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 100 * 10000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        EXPECT_EQ(*data, i * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, KeepsData) {
    setup();
    std::vector<KEY> keys;
    for(KEY key = 0; key < 1000; key++){
        keys.push_back(key);
    }
    auto rng = std::default_random_engine();
    std::shuffle(keys.begin(), keys.end(), rng);
    {
        BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
        for(KEY key : keys){
            tree.insert(key, key * 2);
        }
        // destructor
    }
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    for(KEY key : keys){
        EXPECT_TRUE(tree.contains(key));
    }
    for(KEY key : keys){
        auto data = std::move(tree.find(key));
        EXPECT_TRUE(data);
        EXPECT_EQ(*data, key * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, Update) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    for(size_t i = 0; i < 1000; i++){
        tree.insert(i, i);
    }
    EXPECT_TRUE(tree.update(50, [](DATA& data){
        data = 42;
    }));
    EXPECT_FALSE(tree.update(1001, [](DATA& data){
        data = 42;
    }));
}
// --------------------------------------------------------------------------
TEST(BTree, UpdateLarge) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<KEY> keys;
    for(size_t i = 0; i < 50 * 1000; i++){
        keys.push_back(i);
    }
    auto rng = std::default_random_engine();
    std::shuffle(keys.begin(), keys.end(), rng);
    for(size_t i = 0; i < keys.size(); i++){
        tree.insert(keys[i], keys[i]);
    }
    for(KEY key = 0; key < 50 * 1000; key++){
        EXPECT_TRUE(tree.update(key, [](DATA& data){
            data = data * 2;
        }));
        EXPECT_FALSE(tree.update(key + 50 * 1000, [](DATA& data){
            data = data * 2;
        }));
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        EXPECT_EQ(*data, i * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_1) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<KEY> keys;
    for(size_t i = 0; i < 50 * 1000; i++){
        keys.push_back(i);
    }
    auto rng = std::default_random_engine();
    std::shuffle(keys.begin(), keys.end(), rng);
    for(const KEY& key : keys){
        tree.insert(key, key);
    }
    vector<thread> threads;
    for(size_t i = 0; i < 50 * 1000; i += 1000){
        threads.emplace_back([&tree, i](){
            for(uint32_t key = i; key < i + 1000; key++){
                EXPECT_TRUE(tree.update(key, [](DATA& data){
                    data = data * 2;
                }));
                EXPECT_FALSE(tree.update(key + 50 * 1000, [](DATA& data){
                    data = data * 2;
                }));
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        EXPECT_EQ(*data, i * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_2) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<thread> threads;
    for(size_t i = 0; i < 50 * 1000; i += 1000){
        threads.emplace_back([&tree, i](){
            for(uint32_t key = i; key < i + 1000; key++){
                tree.insert(key, key * 2);
                ASSERT_TRUE(tree.contains(key));
                EXPECT_TRUE(tree.update(key, [](DATA& data){
                    data = data * 2;
                }));
                EXPECT_FALSE(tree.update(key + 50 * 1000, [](DATA& data){
                    data = data * 2;
                }));
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        ASSERT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        EXPECT_EQ(*data, i * 2 * 2);
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_3) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    for(size_t i = 0; i < 500; i++){
        tree.insert(i, i * 2);
    }
    vector<thread> threads;
    for(size_t i = 0; i < 50; i++){
        threads.emplace_back([&tree, i](){
            // high contention
            KEY key = rand() % 10;
            for(size_t j = 0; j < 2500; j++){
                EXPECT_TRUE(tree.update(key, [](DATA& data){
                    data++;
                }));
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_4_Both) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<thread> threads;
    array<size_t, 10> COUNTER;
    COUNTER.fill(0);
    for(KEY key = 0; key < 10; key++){
        tree.insert(key, key);
    }
    for(size_t i = 0; i < 50 * 1000; i += 1000){
        KEY threadKey = rand() % 10;
        COUNTER[threadKey]++;
        threads.emplace_back([&tree, i, threadKey](){
            for(uint32_t key = i; key < i + 1000; key++){
                for(size_t j = 0; j < 100; j++){
                    EXPECT_TRUE(tree.update(threadKey, [](DATA& data){
                        data++;
                    }));
                }
                if(key < 10){
                    continue;
                }
                tree.insert(key, key);
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        if(i < 10){
            EXPECT_EQ(*data, i + COUNTER[i] * 1000 * 100);
        }else{
            EXPECT_EQ(*data, i);
        }
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_4_ContentionOnly) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, false);
    vector<thread> threads;
    array<size_t, 10> COUNTER;
    COUNTER.fill(0);
    for(KEY key = 0; key < 10; key++){
        tree.insert(key, key);
    }
    for(size_t i = 0; i < 50 * 1000; i += 1000){
        KEY threadKey = rand() % 10;
        COUNTER[threadKey]++;
        threads.emplace_back([&tree, i, threadKey](){
            for(uint32_t key = i; key < i + 1000; key++){
                for(size_t j = 0; j < 100; j++){
                    EXPECT_TRUE(tree.update(threadKey, [](DATA& data){
                        data++;
                    }));
                }
                if(key < 10){
                    continue;
                }
                tree.insert(key, key);
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        if(i < 10){
            EXPECT_EQ(*data, i + COUNTER[i] * 1000 * 100);
        }else{
            EXPECT_EQ(*data, i);
        }
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_4_XMergeOnly) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, false, true);
    vector<thread> threads;
    array<size_t, 10> COUNTER;
    COUNTER.fill(0);
    for(KEY key = 0; key < 10; key++){
        tree.insert(key, key);
    }
    for(size_t i = 0; i < 50 * 1000; i += 1000){
        KEY threadKey = rand() % 10;
        COUNTER[threadKey]++;
        threads.emplace_back([&tree, i, threadKey](){
            for(uint32_t key = i; key < i + 1000; key++){
                for(size_t j = 0; j < 100; j++){
                    EXPECT_TRUE(tree.update(threadKey, [](DATA& data){
                        data++;
                    }));
                }
                if(key < 10){
                    continue;
                }
                tree.insert(key, key);
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        if(i < 10){
            EXPECT_EQ(*data, i + COUNTER[i] * 1000 * 100);
        }else{
            EXPECT_EQ(*data, i);
        }
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdate_4_Normal) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, false, false);
    vector<thread> threads;
    array<size_t, 10> COUNTER;
    COUNTER.fill(0);
    for(KEY key = 0; key < 10; key++){
        tree.insert(key, key);
    }
    for(size_t i = 0; i < 50 * 1000; i += 1000){
        KEY threadKey = rand() % 10;
        COUNTER[threadKey]++;
        threads.emplace_back([&tree, i, threadKey](){
            for(uint32_t key = i; key < i + 1000; key++){
                for(size_t j = 0; j < 100; j++){
                    EXPECT_TRUE(tree.update(threadKey, [](DATA& data){
                        data++;
                    }));
                }
                if(key < 10){
                    continue;
                }
                tree.insert(key, key);
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
    for(size_t i = 0; i < 50 * 1000; i++){
        EXPECT_TRUE(tree.contains(i));
        auto data = std::move(tree.find(i));
        if(i < 10){
            EXPECT_EQ(*data, i + COUNTER[i] * 1000 * 100);
        }else{
            EXPECT_EQ(*data, i);
        }
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdateString_1) {
    setup();
    using DATA = array<char, 128>;
    BTree<KEY, DATA, 500, 1024> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<thread> threads;

    for(size_t i = 0; i < 1 * 1000000; i += 1000000){
        threads.emplace_back([&tree, i](){
            for(uint32_t key = i; key < i + 1000000; key++){
                tree.insert(key, std::move(generateRandomString()));
                tree.update(key, [key](DATA& data){
                    data.fill(key % 100);
                });
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
}
// --------------------------------------------------------------------------
TEST(BTree, MultiThreadedUpdateString_2) {
    setup();
    using DATA = array<char, 128>;
    BTree<KEY, DATA, 500, 1024> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    vector<thread> threads;
    for(size_t i = 0; i < 4 * 250000; i += 250000){
        threads.emplace_back([&tree, i](){
            for(uint32_t key = i; key < i + 250000; key++){
                tree.insert(key, std::move(generateRandomString()));
                tree.update(key, [key](DATA& data){
                    data.fill(key % 100);
                });
            }
        });
    }
    for(auto& t : threads){
        t.join();
    }
}
// --------------------------------------------------------------------------
TEST(BTree, CheckSize) {
    setup();
    BTree<KEY, DATA, PAGE_AMOUNT, TOTAL_PAGE_SIZE> tree(BTREE_FILENAME, DATA_FILENAME, true, true);
    std::vector<KEY> keys;
    for(KEY key = 0; key < 1000; key++){
        keys.push_back(key);
    }
    auto rng = std::default_random_engine();
    std::shuffle(keys.begin(), keys.end(), rng);
    for(KEY key : keys){
        tree.insert(key, key * 2);
    }
    // tree was built, check
    EXPECT_EQ(tree.size(), 1000);
}