#ifndef FILE_SUFFIX_STORE_H
#define FILE_SUFFIX_STORE_H

// Succinct stuff
#include "utils/definitions.h"
#include "succinct_base.h"

#include "utils.h"

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#define ss_lookupSA(i)     (accessBMArray(SA, i, bits))

// SuffixStore with a flat file interface.
class FileSuffixStore {
public:

    FileSuffixStore(const std::string& input_file)
        : input_file_(input_file)
    { }

    ~FileSuffixStore() {
        if (SA != nullptr) {
            delete SA;
        }
    }

    void construct();

    void load();

    void search(std::vector<int64_t>& result, const std::string& substring);

    void extract(std::string& ret, int64_t off, int64_t len);

    // Returns the offset after delim.
    int64_t skip_until(int64_t off, unsigned char delim);

    int64_t extract_until(std::string& ret, int64_t off, unsigned char delim);

private:

    SuccinctBase::Bitmap *SA = nullptr;
    long sa_n;
    int bits;

    const std::string input_file_;

    // Common for SuffixStore and LogStore

    void writeSuffixStoreToFile(const char *suffixstore_path);
    void readSuffixStoreFromFile(const char *suffixstore_path);

    void createBMArray(SuccinctBase::Bitmap **B, long *A, long n, int b);
    void initBitmap(SuccinctBase::Bitmap **B, long n);
    void setBMArray(SuccinctBase::Bitmap **B, long i, long val, int b);

    std::pair<long, long> ss_getRange(const char *p);

    long accessBMArray(SuccinctBase::Bitmap *B, long i, int b);

    int ss_compare(const char *p, long i);

    // For Log Store and Suffix Store
    uint8_t *data = nullptr;

};

#endif
