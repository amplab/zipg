#include "FileSuffixStore.h"

#include "utils/divsufsortxx.h"
#include "utils/divsufsortxx_utility.h"

/* Check if n is a power of 2 */
#define ISPOWOF2(n)     ((n != 0) && ((n & (n - 1)) == 0))

void FileSuffixStore::init(int option) {
    /* Integer logarithm to the base 2 -- fast */
    auto intLog2 = [](long n) {
        int l = ISPOWOF2(n) ? 0 : 1;
        while (n >>= 1) ++l;
        return l;
    };

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

void FileSuffixStore::writeSuffixStoreToFile(const char *suffixstore_path) {
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

void FileSuffixStore::readSuffixStoreFromFile(const char *suffixstore_path) {
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
void FileSuffixStore::createBMArray(
    SuccinctBase::Bitmap **B, long *A, long n, int b)
{
    initBitmap(B, n * b);
    for (long i = 0; i < n; i++) {
        setBMArray(B, i, A[i], b);
    }
    std::cout << std::endl;
}

/* Creates a bitmap, initialized to all zeros */
void FileSuffixStore::initBitmap(SuccinctBase::Bitmap **B, long n) {
    (*B)->bitmap = new uint64_t[(n / 64) + 1]();
    (*B)->size = n;
}

/* Set bit in BitMap at pos */
void FileSuffixStore::setBMArray(
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

int FileSuffixStore::ss_compare(const char *p, long i) {
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
long FileSuffixStore::accessBMArray(SuccinctBase::Bitmap *B, long i, int b) {

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

std::pair<long, long> FileSuffixStore::ss_getRange(const char *p) {
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

void FileSuffixStore::search(
    std::vector<int64_t>& result, const std::string& substring)
{
    result.clear();
    COND_LOG_E("search key '%s' (size %d)\n",
        substring.c_str(), substring.length());

    std::pair<long, long> range(ss_getRange(substring.c_str()));
    if (range.first > range.second) {
        return;
    }

    COND_LOG_E("Range: %lu, %lu\n", range.first, range.second);

    result.resize((size_t) range.second - range.first + 1);
    for (int64_t i = range.first; i <= range.second; ++i) {
        result[i - range.first] = static_cast<int64_t>(ss_lookupSA(i));
    }
}

// TODO: dangerous, there is no bound check
void FileSuffixStore::extract(std::string& ret, int64_t off, int64_t len) {
    ret.resize(len);
    for (size_t i = off; i < off + len; ++i) {
        ret[i - off] = data[i];
    }
}

int64_t FileSuffixStore::skip_until(int64_t off, unsigned char delim) {
    while (delim != data[off]) {
        ++off;
    }
    return off + 1;
}

int64_t FileSuffixStore::extract_until(
    std::string& ret, int64_t off, unsigned char delim)
{
    ret.clear();
    while (delim != data[off]) {
        ret += data[off];
        ++off;
    }
    return off + 1;
}
