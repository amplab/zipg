#ifndef KV_LOG_STORE_H
#define KV_LOG_STORE_H

// Succinct stuff
#include "utils/definitions.h"
#include "succinct_base.h"

#include "utils.h"

#include <boost/thread.hpp>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

class Hash {
 public:
  static const uint32_t K1 = 256;
  static const uint32_t K2 = 65536;
  static const uint32_t K3 = 16777216;

  static uint32_t simple_hash2(const char* buf) {
    return buf[0] * K1 + buf[1];
  }

  static uint32_t simple_hash3(const char* buf) {
    return buf[0] * K2 + buf[1] * K1 + buf[2];
  }

  static uint32_t simple_hash4(const char* buf) {
    return buf[0] * K3 + buf[1] * K2 + buf[2] * K1 + buf[3];
  }
};

// LogStore with a key-value interface.
// FIXME: search() has the prefix-match bug.
class KVLogStore {
public:
	static const uint32_t kLogStoreSize = 125 * 1024 * 1024; // 125MB
	typedef std::unordered_map<uint32_t, std::vector<uint32_t>> NGramIdx;

    KVLogStore(int64_t start_key) {
    	cur_key_ = start_key;
    	data_ = new char[kLogStoreSize];
    }

    ~KVLogStore() {
        if (data_ != nullptr) {
            delete [] data_;
        }
    }

    // Thread-safe for concurrent writes.
    int64_t append(const std::string& value);

    // Clears `_return` for caller.
    void search(std::set<int64_t> &_return, const std::string& substring);

    // Clears `value` for caller.
    void get_value(std::string &value, uint64_t key);

private:
    int64_t get_value_offset_pos(const int64_t key);

    int64_t get_key_pos(const int64_t value_offset);

    char* data_;

    // Only for log store
    uint64_t tail_ = static_cast<uint64_t>(0);

    // Index to speed up searches
    // Note: Index only works when logstore data is < 2GB; for larger log
    // stores, switch to long offsets.
    NGramIdx ngram_idx_;
    uint32_t ngram_n_ = 3;

    std::vector<int64_t> keys_;
    std::vector<int32_t> value_offsets_;
    int64_t cur_key_;

    boost::shared_mutex mutex_;
};

#endif
