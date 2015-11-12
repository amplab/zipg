#ifndef KV_SUFFIX_STORE_H
#define KV_SUFFIX_STORE_H

// Succinct stuff
#include "utils/definitions.h"
#include "succinct_base.h"

#include "utils.h"

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#define ss_lookupSA(i)     (accessBMArray(SA, i, bits))

// SuffixStore with a key-value interface.
class KVSuffixStore {
public:

    // The `input_file` should be properly delimed, and with property lengths
    // calculated and attached.  In other words, it should be the content of
    // final input into `new SuccinctShard()`.
    KVSuffixStore(const std::string& input_file,
                  const std::string& pointer_file = "")
        : input_file_(input_file),
          pointer_file_(pointer_file)
    { }

    ~KVSuffixStore() {
        if (SA != nullptr) {
            delete SA;
        }
    }

    // 1 for initialize SA, 2 for also write it out. Otherwise, read from file.
    void init(int option = 1);

    void search(std::set<int64_t> &_return, const std::string& substring);

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

    SuccinctBase::Bitmap *SA = nullptr;
    long sa_n;
    int bits;

    const std::string input_file_, pointer_file_;

    // Common for SuffixStore and LogStore
    int64_t get_value_offset_pos(const int64_t key);
    int64_t get_key_pos(const int64_t value_offset);

    void writeSuffixStoreToFile(const char *suffixstore_path);
    void readSuffixStoreFromFile(const char *suffixstore_path);

    void createBMArray(SuccinctBase::Bitmap **B, long *A, long n, int b);
    void initBitmap(SuccinctBase::Bitmap **B, long n);
    void setBMArray(SuccinctBase::Bitmap **B, long i, long val, int b);

    std::pair<long, long> ss_getRange(const char *p);

    long accessBMArray(SuccinctBase::Bitmap *B, long i, int b);

    int ss_compare(const char *p, long i);

    // For Log Store and Suffix Store
    char *data = nullptr;

    std::vector<long> keys;
    std::vector<long> value_offsets;


};

#endif
