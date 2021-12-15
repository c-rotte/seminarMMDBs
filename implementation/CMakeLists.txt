cmake_minimum_required(VERSION 3.18)
project(btree)

# enable compiler warnings + optimizations (gcc)
string(APPEND CMAKE_CXX_FLAGS "-Wall -Wextra -O3")

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
include(Infrastructure)

add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(ycsb)