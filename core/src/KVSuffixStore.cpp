#include "KVSuffixStore.h"

int64_t KVSuffixStore::get_value_offset_pos(const int64_t key) {
    long pos = std::lower_bound(
        keys.begin(), keys.end(), key) - keys.begin();
    return (keys[pos] != key || pos >= keys.size() ||
        ACCESSBIT(invalid_offsets, pos) == 1) ? -1 : pos;
}

int64_t KVSuffixStore::get_key_pos(const int64_t value_offset) {
    long pos = std::prev(std::upper_bound(value_offsets.begin(),
        value_offsets.end(), value_offset)) - value_offsets.begin();
    return (pos >= keys.size() || ACCESSBIT(invalid_offsets, pos) == 1)
        ? -1 : pos;
}

void KVSuffixStore::init(int option) {
    // TODO
}

int KVSuffixStore::ss_compare(const char *p, long i) {
    long j = 0;
    long pos = ss_lookupSA(i);
    do {
        if(p[j] < data[pos])
            return -1;
        else if(p[j] > data[pos])
            return 1;
        j++;
        pos = (pos + 1) % sa_n;
    } while (j < strlen(p));

    return 0;
}

/* Get bit in BitMap at pos */
long KVSuffixStore::accessBMArray(SuccinctBase::Bitmap *B, long i, int b) {

    unsigned long val;
    uint64_t *bitmap = B->bitmap;
    unsigned long s = i * b, e = i * b + (b - 1);
    if((s / 64) == (e / 64)) {
        val = bitmap[s / 64] << (s % 64);
        val = val >> (63 - e % 64 + s % 64);
    } else {
        unsigned long val1 = bitmap[s / 64] << (s % 64);
        unsigned long val2 = bitmap[e / 64] >> (63 - e % 64);
        val1 = val1 >> (s % 64 - (e % 64 + 1));
        val = val1 | val2;
    }

    return val;
}

std::pair<long, long> KVSuffixStore::ss_getRange(const char *p) {
    long st = sa_n - 1;
    long sp = 0;
    long s;
    while(sp < st) {
        s = (sp + st) / 2;
        if (ss_compare(p, s) > 0) sp = s + 1;
        else st = s;
    }

    long et = sa_n - 1;
    long ep = sp - 1;
    long e;

    while (ep < et) {
        e = ceil((double)(ep + et) / 2);
        if (ss_compare(p, e) == 0) ep = e;
        else et = e - 1;
    }

    std::pair<long, long> range(sp, ep);

    return range;
}

void KVSuffixStore::search(
    std::set<int64_t> &_return, const std::string& substring)
{
    _return.clear();
    std::pair<long, long> range;
    range = ss_getRange(substring.c_str());

    // fprintf(stderr, "Range: %lu, %lu\n", range.first, range.second);

    long sp = range.first, ep = range.second;
    for (long i = 0; i < ep - sp + 1; i++) {
        long pos = get_key_pos(ss_lookupSA(sp + i));
        if(pos >= 0)
            _return.insert(keys[pos]);
    }
}

void KVSuffixStore::get_value(std::string &value, uint64_t key) {
    value = "";
    int64_t pos = get_value_offset_pos(key);
    if(pos < 0)
        return;
    int64_t start = value_offsets[pos];
    int64_t end = (pos + 1 < value_offsets.size()) ? value_offsets[pos + 1] : sa_n - 1;
    value.resize(end - start);
    for(int64_t i = start; i < end; i++)
        value[i - start] = data[i];
}
