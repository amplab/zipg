#include "FileLogStore.h"

#include <algorithm>

void FileLogStore::construct() {
    data = new char[MAX_LOG_STORE_SIZE];
    // just the values
    read_data(input_file_.c_str());

    create_ngram_idx();

    writeLogStoreToFile((input_file_ + "_logstore").c_str());
    std::cout << "FileLogStore: wrote to file "
        << (input_file_ + "_logstore").c_str() << std::endl;
}

void FileLogStore::load() {
    readLogStoreFromFile((input_file_ + "_logstore").c_str());
}

void FileLogStore::writeLogStoreToFile(const char* logstore_path) {
    std::ofstream logstore_file(logstore_path);
    logstore_file << data_pos << std::endl;
    logstore_file << ngram_n << std::endl;

    // Write char array to file
    logstore_file.write(data, data_pos);

    // Write ngram_idx to file
    logstore_file << ngram_idx.size() << std::endl;
    for (std::unordered_map<std::string, std::vector<uint32_t> >::iterator it = ngram_idx.begin(); it != ngram_idx.end(); ++it) {
        // Write string
        for(uint32_t i = 0; i < ngram_n; i++) {
            logstore_file << (int)it->first[i] << " ";
        }
        logstore_file << it->second.size() << std::endl;
        for(uint64_t i = 0; i < it->second.size(); i++) {
            logstore_file << it->second[i] << " ";
        }
        logstore_file << "\n";
    }

    std::cout << "Wrote ngram_idx to file!" << std::endl;
    logstore_file.close();
}

void FileLogStore::readLogStoreFromFile(const char* logstore_path) {
    if (file_or_dir_exists(logstore_path)) {
        std::ifstream logstore_file(logstore_path);
        logstore_file >> data_pos;
        std::cout << "data_pos = " << data_pos << std::endl;
        logstore_file >> ngram_n;
        std::cout << "ngram_n = " << ngram_n << std::endl;

        logstore_file.ignore();

        data = new char[MAX_LOG_STORE_SIZE];

        // Read char array from file
        logstore_file.read(data, data_pos);

        // Read ngram_idx from file
        size_t ngram_idx_size;
        logstore_file >> ngram_idx_size;

        std::cout << "ngram index size = " << ngram_idx_size << std::endl;

        uint64_t num_offsets = 0;
        for (size_t entry = 0; entry < ngram_idx_size; entry++) {
            // Read string
            std::string ngram = "";
            for(uint32_t i = 0; i < ngram_n; i++) {
                int c;
                logstore_file >> c;
                ngram += ((char)c);
            }

            // Read offsets
            size_t offsets_size;
            logstore_file >> offsets_size;
            for(size_t i = 0; i < offsets_size; i++) {
                uint32_t offset;
                logstore_file >> offset;
                ngram_idx[ngram].push_back(offset);
                num_offsets++;
            }
        }

        std::cout << "Read ngram_idx from file!" << std::endl;
        assert((num_offsets == 0 && data_pos < ngram_n)
            || num_offsets == (data_pos - ngram_n));

        std::cout << "Loaded log store from file!" << std::endl;
    }
}

// Reads into `data`, updating `data_pos` correctly.
void FileLogStore::read_data(const char *input_file) {
    if (file_or_dir_exists(input_file)) {
        std::ifstream ip;
        uint64_t size;

        ip.open(input_file);
        ip.seekg(0, std::ios::end);
        size = ip.tellg();
        ip.seekg (0, std::ios::beg);
        ip.read(data, size);
        ip.close();

        data_pos = size;
        COND_LOG_E("Log store read %llu bytes\n", data_pos);
    }
}

// Log and suffix store initialization functions
void FileLogStore::create_ngram_idx() {
    for(long i = 0; i < (long)data_pos - (long)ngram_n; i++) {
        std::string ngram = "";
        for(uint32_t off = 0; off < ngram_n; off++) {
            // assert(i + off < data_pos);
            ngram += data[i + off];
        }
        ngram_idx[ngram].push_back(i);
        if (i % 1048576 == 0) {
            fprintf(stderr, "Processed %ld bytes of data.\n", i);
        }
    }
    std::cout << "Created ngram idx! Size = "
        << ngram_idx.size() * (ngram_n + 8) + data_pos * 4
        << "; num entries = " << ngram_idx.size() << "\n";
}

int32_t FileLogStore::append(const std::string& value) {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);

    if (data_pos + value.length() > MAX_LOG_STORE_SIZE) {
        return -1;   // Data exceeds max chunk size
    }
    strncpy(data + data_pos, value.c_str(), value.length());

    // Update the index

    // max with 0, since data_pos can be small (or zero) initially
    for (int64_t i = std::max(0LL, static_cast<int64_t>(data_pos - ngram_n));
        i <= data_pos + value.length() - ngram_n; i++)
    {
        std::string ngram;
        for(uint32_t off = 0; off < ngram_n; off++) {
            ngram += data[i + off];
        }
        ngram_idx[ngram].push_back(i);
    }
    data_pos += value.length();
    return 0;
}

void FileLogStore::search(
    std::vector<int64_t> &_return, const std::string& substring)
{
    _return.clear();
    COND_LOG_E("search string '%s' (size %d)\n",
        substring.c_str(), substring.length());

    std::string substring_ngram = substring.substr(0, ngram_n);
    char *substr = (char *)substring.c_str();
    char *suffix = substr + ngram_n;

    boost::shared_lock<boost::shared_mutex> lk(mutex_);

    std::vector<uint32_t> idx_offsets = ngram_idx[substring_ngram];

    COND_LOG_E("idx sizes: %d, substring '%s'\n", idx_offsets.size(),
        substring_ngram.c_str());

    for (uint32_t i = 0; i < idx_offsets.size(); i++) {
        if (strncmp(data + idx_offsets[i] + ngram_n,
                    suffix,
                    substring.length() - ngram_n) == 0)
        {
            _return.push_back(idx_offsets[i]);
        }
    }
}

void FileLogStore::extract(std::string& result, uint64_t offset, uint64_t len) {
    result.clear();
    int64_t start = offset;

    boost::shared_lock<boost::shared_mutex> lk(mutex_);

    int64_t end = offset + len < data_pos ? offset + len : data_pos;
    size_t l = end - start;
    result.resize(l);
    COND_LOG_E("start = %lld, end = %lld\n", start, end);
    for (size_t i = 0; i < l; ++i) {
        COND_LOG_E("i = %lld\n", i);
        result[i] = data[start + i];
    }
}

int64_t FileLogStore::skip_until(int64_t off, unsigned char delim) {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    while (delim != data[off]) {
        ++off;
    }
    return off + 1;
}

int64_t FileLogStore::extract_until(
    std::string& ret, int64_t off, unsigned char delim)
{
    ret.clear();
    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    while (delim != data[off]) {
        ret += data[off];
        ++off;
    }
    return off + 1;
}
