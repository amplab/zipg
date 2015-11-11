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

// LogStore with a key-value interface.
class KVLogStore {
public:

    KVLogStore(const std::string& input_file)
        : input_file_(input_file)
    { }

    // Reads in file, build ngram index, etc.
    void init();

    int32_t append(int64_t key, std::string& value);

    void search(std::set<int64_t> &_return, const std::string& substring);

    void get_value(std::string &value, uint64_t key);

private:

    const std::string input_file_;

    int64_t get_value_offset_pos(const int64_t key);

    int64_t get_key_pos(const int64_t value_offset);

    // For Log Store and Suffix Store
    char *data;

    // Only for log store
    uint64_t data_pos;

    // Index to speed up searches
    // Note: Index only works when logstore data is < 2GB; for larger log
    // stores, switch to long offsets.
    std::unordered_map<std::string, std::vector<uint32_t> > ngram_idx;
    uint32_t ngram_n;

    char delim;
    std::vector<long> keys;
    std::vector<long> value_offsets;

    /* Data Structures */
    /* Bit-map data structure */
    typedef struct _bitmap {
        uint64_t *bitmap;
        long size;
    } BitMap;

    BitMap *invalid_offsets;

};

#endif
