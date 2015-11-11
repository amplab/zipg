#ifndef KV_LOG_STORE_H
#define KV_LOG_STORE_H

#include <unordered_map>

// LogStore with a key-value interface.
class KVLogStore {
public:

    KVLogStore(const std::string& input_file)
        : input_file_(input_file)
    { }

    void init();

    int32_t append(int64_t key, const std::string& value);

    void search(std::set<int64_t> &_return, const std::string& substring);

    void get_value(std::string &value, uint64_t key);

private:

    const std::string input_file_;

    int64_t get_value_offset_pos(const int64_t key);

    int64_t get_key_pos(const int64_t value_offset) {
        long pos = std::prev(std::upper_bound(value_offsets.begin(),
            value_offsets.end(), value_offset)) - value_offsets.begin();
        return (pos >= keys.size() || ACCESSBIT(invalid_offsets, pos) == 1)
            ? -1 : pos;
    }

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
