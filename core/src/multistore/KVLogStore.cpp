#include "multistore/KVLogStore.h"

int64_t KVLogStore::get_value_offset_pos(const int64_t key) {
    long pos = std::lower_bound(
        keys.begin(), keys.end(), key) - keys.begin();
    return (keys[pos] != key || pos >= keys.size() ||
        ACCESSBIT(invalid_offsets, pos) == 1) ? -1 : pos;
}
