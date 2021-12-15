set(BTREE_SOURCES
        btree/BTree.cpp)

add_library(btree_core ${BTREE_SOURCES})
target_include_directories(btree_core PUBLIC ${CMAKE_SOURCE_DIR})

add_clang_tidy_target(lint_btree_core main.cpp)
add_dependencies(lint lint_btree_core)

add_executable(btree main.cpp)
target_link_libraries(btree PUBLIC btree_core)

