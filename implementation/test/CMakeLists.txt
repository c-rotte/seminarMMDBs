set(TEST_SOURCES
        Tester.cpp
        TestDiskManager.cpp
        TestBufferManager.cpp
        TestBTree.cpp)

add_executable(tester ${TEST_SOURCES})
target_link_libraries(tester PUBLIC
    btree_core
    GTest::GTest)
