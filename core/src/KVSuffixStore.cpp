#include "KVSuffixStore.h"

#include "utils/divsufsortxx.h"
#include "utils/divsufsortxx_utility.h"

/* Check if n is a power of 2 */
#define ISPOWOF2(n)     ((n != 0) && ((n & (n - 1)) == 0))
/* Integer logarithm to the base 2 -- fast */
int intLog2(long n) {
    int l = ISPOWOF2(n) ? 0 : 1;
    while (n >>= 1) ++l;
    return l;
}

int64_t KVSuffixStore::get_value_offset_pos(const int64_t key) {
    long pos = std::lower_bound(
        keys.begin(), keys.end(), key) - keys.begin();
    return (keys[pos] != key || pos >= keys.size()) ? -1 : pos;
}

int64_t KVSuffixStore::get_key_pos(const int64_t value_offset) {
    long pos = std::prev(std::upper_bound(value_offsets.begin(),
        value_offsets.end(), value_offset)) - value_offsets.begin();
    return pos >= keys.size() ? -1 : pos;
}

void KVSuffixStore::init(int option) {
    if (option == 1 || option == 2) {
        std::ifstream ip;
        ip.open(input_file_.c_str());
        std::string *str = new std::string((std::istreambuf_iterator<char>(ip)),
                                            std::istreambuf_iterator<char>());
        str->append(1, (char)1);

        sa_n = str->length();
        ip.close();
        std::cout << "File read into memory!" << std::endl;
        bits = intLog2(sa_n + 1);

        // Construct suffix array
        long *lSA = new long[sa_n];
        divsufsortxx::constructSA((const unsigned char *)str->c_str(),
            ((const unsigned char *)str->c_str()) + sa_n, lSA, lSA + sa_n, 256);

        std::cout << "Built SA\n";
        SA = new SuccinctBase::Bitmap;
        createBMArray(&SA, lSA, sa_n, bits);
        delete [] lSA;
        std::cout << "Compacted SA\n";

        data = (uint8_t *) str->c_str();

        if (pointer_file_ != "") {
            read_pointers(pointer_file_.c_str());
        } else {
            build_pointers();
        }

        if (option == 2) {
            writeSuffixStoreToFile((input_file_ + "_suffixstore").c_str());
            std::cout << "Wrote suffix store to file "
                << (input_file_ + "_suffixstore").c_str() << std::endl;
        }
    } else {
        // Read from file
        readSuffixStoreFromFile(input_file_.c_str());
        std::cout << "Loaded suffix store from file!" << std::endl;
    }
}

void KVSuffixStore::writeSuffixStoreToFile(const char *suffixstore_path) {
    std::ofstream suffixstore_file(suffixstore_path);
    suffixstore_file << sa_n << std::endl;
    suffixstore_file << bits << std::endl;

    // Write char array to file
    suffixstore_file.write((char*) data, sa_n);

    // Write suffix array to file
    suffixstore_file << SA->size << std::endl;

    for(long i = 0; i < (SA->size / 64) + 1; i++) {
        suffixstore_file << SA->bitmap[i] << " ";
    }
    suffixstore_file << std::endl;

    std::cout << "Wrote suffix array to file!" << std::endl;
    suffixstore_file.close();
}

void KVSuffixStore::readSuffixStoreFromFile(const char *suffixstore_path) {
    std::ifstream suffixstore_file(suffixstore_path);
    suffixstore_file >> sa_n;
    suffixstore_file >> bits;

    suffixstore_file.ignore();
    data = new uint8_t[sa_n];

    // Read char array from file
    suffixstore_file.read((char*) data, sa_n);

    // Read suffix array from file
    SA = new SuccinctBase::Bitmap;
    suffixstore_file >> SA->size;
    SA->bitmap = new uint64_t[(SA->size / 64) + 1];
    for(long i = 0; i < (SA->size / 64) + 1; i++) {
        suffixstore_file >> SA->bitmap[i];
    }

    suffixstore_file.close();
}

/* Creates bitmap array */
void KVSuffixStore::createBMArray(
    SuccinctBase::Bitmap **B, long *A, long n, int b)
{
    initBitmap(B, n * b);
    for (long i = 0; i < n; i++) {
        setBMArray(B, i, A[i], b);
    }
    std::cout << std::endl;
}

/* Creates a bitmap, initialized to all zeros */
void KVSuffixStore::initBitmap(SuccinctBase::Bitmap **B, long n) {
    (*B)->bitmap = new uint64_t[(n / 64) + 1]();
    (*B)->size = n;
}

/* Set bit in BitMap at pos */
void KVSuffixStore::setBMArray(
    SuccinctBase::Bitmap **B, long i, long val, int b)
{
    unsigned long s = i * b, e = i * b + (b - 1);
    if ((s / 64) == (e / 64)) {
        (*B)->bitmap[s / 64] |= (val << (63 - e % 64));
    } else {
        (*B)->bitmap[s / 64] |= (val >> (e % 64 + 1));
        (*B)->bitmap[e / 64] |= (val << (63 - e % 64));
    }
}

int KVSuffixStore::ss_compare(const char *p, long i) {
    long j = 0;
    long pos = ss_lookupSA(i);
    do {
        if(static_cast<uint8_t>(p[j]) < data[pos])
            return -1;
        else if(static_cast<uint8_t>(p[j]) > data[pos])
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
    COND_LOG_E("start %ld, end %ld, search key '%s' (size %d)\n",
        sp, ep, substring.c_str(), substring.length());
    for (long i = 0; i < ep - sp + 1; i++) {
        long pos = get_key_pos(ss_lookupSA(sp + i));
        if(pos >= 0)
            _return.insert(keys[pos]);
    }
}

void KVSuffixStore::get_value(std::string &value, uint64_t key) {
    value.clear();
    int64_t pos = get_value_offset_pos(key);
    COND_LOG_E("get_value_offset_pos done: %lld; key %lld, "
        "value_offsets.size %d\n",
        pos, key, value_offsets.size());
    if (pos < 0) {
        return;
    }
    int64_t start = value_offsets[pos];
    int64_t end = (pos + 1 < value_offsets.size())
        ? value_offsets[pos + 1] : sa_n - 1;
    size_t len = end - start - 1; // -1 for ignoring the delim
    value.resize(len);
    for (size_t i = 0; i < len; ++i) {
        value[i] = data[start + i];
    }
}
