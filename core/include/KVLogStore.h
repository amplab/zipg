#ifndef KV_LOG_STORE_H
#define KV_LOG_STORE_H

// Succinct stuff
#include "utils/definitions.h"
#include "succinct_base.h"

#include "utils.h"

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

constexpr int MAX_LOG_STORE_SIZE = 131072000; // 125MB

// LogStore with a key-value interface.
// FIXME: search() has the prefix-match bug.
class KVLogStore {
public:

    KVLogStore(const std::string& input_file, const std::string& pointer_file)
        : input_file_(input_file),
          pointer_file_(pointer_file)
    { }

    ~KVLogStore() {
        if (data != nullptr) {
            delete [] data;
        }
    }

    // Reads in file, build ngram index, etc.
    void init(int option = 1);

    int32_t append(int64_t key, const std::string& value);

    // Clears `_return` for caller.
    void search(std::set<int64_t> &_return, const std::string& substring);

    // Clears `value` for caller.
    void get_value(std::string &value, uint64_t key);

private:

    void read_pointers(const char *ptrs_file) {
        std::ifstream ip;
        ip.open(ptrs_file);
        std::string line;
        std::string key, value;
        long line_num = 0;
        while (std::getline(ip, line)) {
            uint32_t kv_split_index = line.find_first_of('\t');
            key = line.substr(0, kv_split_index);
            value = line.substr(kv_split_index + 1);
            keys.push_back(std::stoll(key));
            value_offsets.push_back(atol(value.c_str()));
            line_num++;
        }
        LOG_E("Read %lld KV pointers.\n", keys.size());
    }

    int64_t get_value_offset_pos(const int64_t key);

    int64_t get_key_pos(const int64_t value_offset);

    void readLogStoreFromFile(const char* logstore_path);
    void writeLogStoreToFile(const char* logstore_path);

    void read_data(const char *input_file);
    void create_ngram_idx();

    const std::string input_file_, pointer_file_;

    // For Log Store and Suffix Store
    // shared_ptr<char*> data = nullptr;
    char* data = nullptr;

    // Only for log store
    uint64_t data_pos = 0;

    // Index to speed up searches
    // Note: Index only works when logstore data is < 2GB; for larger log
    // stores, switch to long offsets.
    std::unordered_map<std::string, std::vector<uint32_t> > ngram_idx;
    uint32_t ngram_n = 3;

    static constexpr char delim = (char) 10;
    std::vector<long> keys;
    std::vector<long> value_offsets;

};

#endif
