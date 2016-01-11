#ifndef FILE_LOG_STORE_H
#define FILE_LOG_STORE_H

// Succinct stuff
#include "utils/definitions.h"
#include "succinct_base.h"

#include "utils.h"

#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// LogStore with a flat file interface.
// FIXME: search() has the prefix-match bug?
class FileLogStore {
public:

    FileLogStore(const std::string& input_file)
        : input_file_(input_file)
    { }

    ~FileLogStore() {
        if (data != nullptr) {
            delete [] data;
        }
    }

    // Reads in file and builds ngram index.  If `pointer_file_` is empty
    // string, builds pointers on the fly by scanning the input, with newlines
    // acting as record delimiters.
    void construct();

    void load();

    // Thread-safe for concurrent writes.
    int32_t append(const std::string& value);

    // Clears `_return` for caller.
    void search(std::vector<int64_t> &_return, const std::string& substring);

    void extract(std::string& result, uint64_t offset, uint64_t len);

private:

    constexpr static int MAX_LOG_STORE_SIZE = 131072000; // 125MB

    void readLogStoreFromFile(const char* logstore_path);
    void writeLogStoreToFile(const char* logstore_path);

    void read_data(const char *input_file);
    void create_ngram_idx();

    const std::string input_file_, pointer_file_;

    // For Log Store and Suffix Store
    // shared_ptr<char*> data = nullptr;
    char* data = nullptr;

    // Only for log store
    uint64_t data_pos = static_cast<uint64_t>(0);

    // Index to speed up searches
    // Note: Index only works when logstore data is < 2GB; for larger log
    // stores, switch to long offsets.
    std::unordered_map<std::string, std::vector<uint32_t> > ngram_idx;
    uint32_t ngram_n = 3;

    static constexpr char delim = '\n';

    std::mutex mutex_;
};

#endif
