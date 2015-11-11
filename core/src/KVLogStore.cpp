#include "KVLogStore.h"

int64_t KVLogStore::get_value_offset_pos(const int64_t key) {
    long pos = std::lower_bound(
        keys.begin(), keys.end(), key) - keys.begin();
    return (keys[pos] != key || pos >= keys.size() ||
        ACCESSBIT(invalid_offsets, pos) == 1) ? -1 : pos;
}

int64_t KVLogStore::get_key_pos(const int64_t value_offset) {
    long pos = std::prev(std::upper_bound(value_offsets.begin(),
        value_offsets.end(), value_offset)) - value_offsets.begin();
    return (pos >= keys.size() || ACCESSBIT(invalid_offsets, pos) == 1)
        ? -1 : pos;
}

void KVLogStore::init() {
    // TODO
}

int32_t KVLogStore::append(int64_t key, std::string& value) {
    if(data_pos + value.length() > MAX_LOG_SIZE) return -1;   // Data exceeds max chunk size
    keys.push_back(key);
    value_offsets.push_back(data_pos);
    value += delim;
    strncpy(data + data_pos, value.c_str(), value.length());
    // Update the index
    for(uint64_t i = data_pos - ngram_n; i < data_pos + value.length() - ngram_n; i++) {
        std::string ngram = "";
        for(uint32_t off = 0; off < ngram_n; off++) {
            ngram += data[i + off];
        }
        ngram_idx[ngram].push_back(i);
    }
    data_pos += value.length();
    return 0;
}

void KVLogStore::search(
    std::set<int64_t> &_return, const std::string& substring)
{
    std::string substring_ngram = substring.substr(0, ngram_n);
    std::vector<uint32_t> idx_offsets = ngram_idx[substring_ngram];

    char *substr = (char *)substring.c_str();
    char *suffix = substr + ngram_n;

    for(uint32_t i = 0; i < idx_offsets.size(); i++) {
        if(strncmp(data + idx_offsets[i] + ngram_n, suffix, substring.length() - ngram_n) == 0) {
            int64_t pos = get_key_pos(idx_offsets[i]);
            if(pos >= 0)
                _return.insert(keys[pos]);
        }
    }
}

void KVLogStore::get_value(std::string &value, uint64_t key) {
    value = "";
    int64_t pos = get_value_offset_pos(key);
    if(pos < 0)
        return;
    uint64_t start = value_offsets[pos];
    uint32_t end = (pos + 1 < value_offsets.size()) ? value_offsets[pos + 1] : data_pos;
    value.resize(end - start);
    for(uint32_t i = start; i < end; i++)
        value[i - start] = data[i];
}
