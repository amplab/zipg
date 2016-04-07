#ifndef KV_LOG_STORE_H
#define KV_LOG_STORE_H

// Succinct stuff
#include "utils/definitions.h"
#include "succinct_base.h"

#include "utils.h"

#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <atomic>

constexpr int MAX_LOG_STORE_SIZE = 131072000; // 125MB

// LogStore with a key-value interface.
// FIXME: search() has the prefix-match bug.
class KVLogStore {
public:

    KVLogStore(const std::string& input_file, const std::string& pointer_file)
        : input_file_(input_file),
          pointer_file_(pointer_file)
    {
    	cur_key = 0;
    }

    KVLogStore(const std::string& input_file)
        : input_file_(input_file),
          pointer_file_("")
    {
    	cur_key = 0;
    }

    ~KVLogStore() {
        if (data != nullptr) {
            delete [] data;
        }
    }

    // Reads in file and builds ngram index.  If `pointer_file_` is empty
    // string, builds pointers on the fly by scanning the input, with newlines
    // acting as record delimiters.
    void construct();

    void load();

    // Thread-safe for concurrent writes.
    int64_t append(const std::string& value);

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
        while (std::getline(ip, line)) {
            uint32_t kv_split_index = line.find_first_of('\t');
            key = line.substr(0, kv_split_index);
            value = line.substr(kv_split_index + 1);
            keys.push_back(std::stoll(key));
            value_offsets.push_back(atol(value.c_str()));
        }
        LOG_E("Read %lld KV pointers.\n", keys.size());
    }

    // Builds pointers by scanning the input.  Uses line numbers (0-based)
    // as keys, and newlines as record delims.
    void build_pointers() {
        keys.clear();
        value_offsets.clear();

        std::ifstream ifstream(input_file_);
        std::string line;
        size_t curr_len = 0, i = 0;
        // treating newlines as record delim, and
        // line numbers as keys
        while (std::getline(ifstream, line)) {
            keys.push_back(i);
            value_offsets.push_back(curr_len);
            curr_len += line.length() + 1; // +1 for stripped newline
            ++i;
        }
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
    uint64_t data_pos = static_cast<uint64_t>(0);

    // Index to speed up searches
    // Note: Index only works when logstore data is < 2GB; for larger log
    // stores, switch to long offsets.
    std::unordered_map<std::string, std::vector<uint32_t> > ngram_idx;
    uint32_t ngram_n = 3;

    static constexpr char delim = '\n';
    std::vector<long> keys;
    std::vector<long> value_offsets;
    long cur_key;


    std::mutex mutex_;
};

#endif
