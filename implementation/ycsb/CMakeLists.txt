set(YCSB_SOURCES
        core/acknowledged_counter_generator.cc
        core/core_workload.cc
        core/db_factory.cc
        core/measurements.cc
        btree/BTreeDB.cpp)

message("${YCSB_SOURCES}")

add_library(ycsb_core ${YCSB_SOURCES})
target_include_directories(ycsb_core PUBLIC ${CMAKE_SOURCE_DIR})

add_executable(ycsb core/ycsbc.cc)
find_package(Threads REQUIRED)
target_link_libraries(ycsb PUBLIC btree_core ycsb_core Threads::Threads)