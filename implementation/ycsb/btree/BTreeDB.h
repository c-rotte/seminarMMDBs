#ifndef BTREE_BTREEDB_H
#define BTREE_BTREEDB_H
// --------------------------------------------------------------------------
#define LOGGING
// --------------------------------------------------------------------------
#include "ycsb/core/db.h"
#include "src/btree/BTree.h"
#include <filesystem>
#include <array>
// --------------------------------------------------------------------------
namespace ycsbc {
// --------------------------------------------------------------------------
template <bool C, bool X>
class BTreeDB : public DB {

    public:
    using KEY = std::array<char, 32>;
    using DATA = std::array<char, 1000>;

    static constexpr size_t PAGES = 75 * 1000;
    static constexpr size_t PAGE_SIZE = 4096;

    public:
    btree::BTree<KEY, DATA, PAGES, PAGE_SIZE>* tree = nullptr;

    public:
    void Init();
    Status Read(const std::string &table, const std::string &key,
                const std::vector<std::string> *fields, std::vector<Field> &result);
    Status Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result);
    Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);
    Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);
    Status Delete(const std::string &table, const std::string &key);

};
// --------------------------------------------------------------------------
template <bool C, bool X>
void BTreeDB<C, X>::Init() {
    std::filesystem::remove("/tmp/tree.txt");
    std::filesystem::remove("/tmp/data.txt");
    tree = new btree::BTree<KEY, DATA, PAGES, PAGE_SIZE>(
        "/tmp/tree.txt", "/tmp/data.txt", C, X);

    if(C){
        tree->d1 = 0.009;
        tree->d2 = 0.0009;
        tree->d3 = 0.9;
    }
}
// --------------------------------------------------------------------------
template <bool C, bool X>
DB::Status BTreeDB<C, X>::Read(const std::string&, const std::string &k,
            const std::vector<std::string> *fields, std::vector<Field>&) {
    KEY key = {};
    memcpy(key.data(), k.c_str(), std::min(k.length(), key.size()));
    auto data = std::move(tree->find(key));
    if(!data){
        return Status::kNotFound;
    }
    return Status::kOK;
}
// --------------------------------------------------------------------------
template <bool C, bool X>
DB::Status BTreeDB<C, X>::Scan(const std::string &table, const std::string &key, int len,
            const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return Status::kNotImplemented;
}
// --------------------------------------------------------------------------
template <bool C, bool X>
DB::Status BTreeDB<C, X>::Update(const std::string&, const std::string &k, std::vector<Field> &values) {
    KEY key = {};
    memcpy(key.data(), k.c_str(), std::min(k.length(), key.size()));
    DATA data = {};
    size_t offset = 0;
    for(Field field : values){
        memcpy(data.data() + offset, field.value.c_str(), field.value.length());
        offset += field.value.length();
    }
    bool success = tree->update(key, [&data](DATA& d){
        d = std::move(data);
    });
    if(!success){
        return Status::kNotFound;
    }
    return Status::kOK;
}
// --------------------------------------------------------------------------
template <bool C, bool X>
DB::Status BTreeDB<C, X>::Insert(const std::string&, const std::string &k, std::vector<Field> &values) {
    KEY key = {};
    memcpy(key.data(), k.c_str(), std::min(k.length(), key.size()));
    DATA data = {};
    size_t offset = 0;
    for(const Field& field : values){
        memcpy(data.data() + offset, field.value.c_str(), field.value.length());
        offset += field.value.length();
    }
    if(offset > data.size()){
        return Status::kError;
    }
    tree->insert(std::move(key), std::move(data));
    return Status::kOK;
}
// --------------------------------------------------------------------------
template <bool C, bool X>
DB::Status BTreeDB<C, X>::Delete(const std::string &table, const std::string &key) {
    return Status::kNotImplemented;
}
// --------------------------------------------------------------------------
template <bool C, bool X>
DB* newBTreeDB() {
    return new BTreeDB<C, X>;
}
// --------------------------------------------------------------------------
} // namespace ycsbc
// --------------------------------------------------------------------------
#endif //BTREE_BTREEDB_H
