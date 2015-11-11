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

    KVSuffixStore(const std::string& input_file)
        : input_file_(input_file)
    { }

    void init();

    void search(std::set<int64_t> &_return, const std::string& substring);

    void get_value(std::string &value, uint64_t key);

private:

    SuccinctBase::Bitmap *SA;
    long sa_n;
    int bits;

    const std::string input_file_;

    // Common for SuffixStore and LogStore
    int64_t get_value_offset_pos(const int64_t key);
    int64_t get_key_pos(const int64_t value_offset);

    std::pair<long, long> ss_getRange(const char *p);

    long accessBMArray(SuccinctBase::Bitmap *B, long i, int b);

    int ss_compare(const char *p, long i);

    // For Log Store and Suffix Store
    char *data;

    std::vector<long> keys;
    std::vector<long> value_offsets;

    SuccinctBase::Bitmap *invalid_offsets;

};

#endif
