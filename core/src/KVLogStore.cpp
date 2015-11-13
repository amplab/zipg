#include "KVLogStore.h"

#include <algorithm>

int64_t KVLogStore::get_value_offset_pos(const int64_t key) {
    long pos = std::lower_bound(
        keys.begin(), keys.end(), key) - keys.begin();
    COND_LOG_E("pos = %d, keys.size = %d\n", pos, keys.size());
    return (keys[pos] != key || pos >= keys.size()) ? -1 : pos;
}

int64_t KVLogStore::get_key_pos(const int64_t value_offset) {
    long pos = std::prev(std::upper_bound(value_offsets.begin(),
        value_offsets.end(), value_offset)) - value_offsets.begin();
    return pos >= keys.size() ? -1 : pos;
}

void KVLogStore::readLogStoreFromFile(const char* logstore_path) {
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
    for(size_t entry = 0; entry < ngram_idx_size; entry++) {
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
    assert((num_offsets == 0 && data_pos < ngram_n) || num_offsets == (data_pos - ngram_n));
    logstore_file.close();
}

void KVLogStore::writeLogStoreToFile(const char* logstore_path) {
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

// Reads into `data`, updating `data_pos` correctly.
void KVLogStore::read_data(const char *input_file) {
    std::ifstream ip;
    uint64_t size;

    ip.open(input_file);
    ip.seekg(0, std::ios::end);
    size = ip.tellg();
    ip.seekg (0, std::ios::beg);
    ip.read(data, size);
    ip.close();

    data_pos = size;
    LOG_E("Log store read %zu bytes\n", data_pos);
}

// Log and suffix store initialization functions
void KVLogStore::create_ngram_idx() {
    for(long i = 0; i < (long)data_pos - (long)ngram_n; i++) {
        std::string ngram = "";
        for(uint32_t off = 0; off < ngram_n; off++) {
            // assert(i + off < data_pos);
            ngram += data[i + off];
        }
        ngram_idx[ngram].push_back(i);
        if(i % 1048576 == 0) {
            fprintf(stderr, "Processed %ld bytes of data.\n", i);
        }
    }
    std::cout << "Created ngram idx! Size = "
        << ngram_idx.size() * (ngram_n + 8) + data_pos * 4
        << "; num entries = " << ngram_idx.size() << "\n";
}

// Option: 1 for initialize; 2 for init and write it out; 3 for read in.
void KVLogStore::init(int option) {
	data = new char[MAX_LOG_STORE_SIZE];
	// just the values
	read_data(input_file_.c_str());
	// format: "[key] \t [offset into the value file]"
	if (pointer_file_ != "") {
	    read_pointers(pointer_file_.c_str());
	} else {
	    build_pointers();
	}
	create_ngram_idx();
    COND_LOG_E("Done ngram index\n");
}

int32_t KVLogStore::append(int64_t key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (data_pos + value.length() > MAX_LOG_STORE_SIZE) {
        return -1;   // Data exceeds max chunk size
    }
    std::string val(value);
    keys.push_back(key);
    value_offsets.push_back(data_pos);
    val += delim;
    strncpy(data + data_pos, val.c_str(), val.length());

    // Update the index

    // min with 0, since data_pos can be small (or zero) initially
    for(uint64_t i = std::min(data_pos - ngram_n, 0ULL);
        i < data_pos + val.length() - ngram_n; i++)
    {
        std::string ngram;
        for(uint32_t off = 0; off < ngram_n; off++) {
            ngram += data[i + off];
        }
        ngram_idx[ngram].push_back(i);
    }
    data_pos += val.length();
    return 0;
}

void KVLogStore::search(
    std::set<int64_t> &_return, const std::string& substring)
{
    _return.clear();
    COND_LOG_E("search string '%s' (size %d)\n",
        substring.c_str(), substring.length());
    std::string substring_ngram = substring.substr(0, ngram_n);
    std::vector<uint32_t> idx_offsets = ngram_idx[substring_ngram];

    COND_LOG_E("idx sizes: %d, substring '%s'\n", idx_offsets.size(),
        substring_ngram.c_str());

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
        ? value_offsets[pos + 1] : data_pos;
    size_t len = end - start - 1; // -1 for ignoring the delim
    value.resize(len);
    for (size_t i = 0; i < len; ++i) {
        value[i] = data[start + i];
    }
}
