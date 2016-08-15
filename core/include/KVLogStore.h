#ifndef KV_LOG_STORE_H
#define KV_LOG_STORE_H

#include "slog/logstore.h"

// LogStore with a key-value interface.
class KVLogStore {
 public:
  KVLogStore(int64_t start_key) {
    start_key_ = start_key;
  }

  ~KVLogStore() {
  }

  // Thread-safe for concurrent writes.
  int64_t append(const std::string& value) {
    return start_key_ + logstore_.insert(value);
  }

  int64_t insert(const int64_t key, const std::string& value) {
    return logstore_.insert(key, value);
  }

  // Clears `_return` for caller.
  void search(std::set<int64_t> &_return, const std::string& substring) {
    logstore_.search(_return, substring);
  }

  // Clears `value` for caller.
  void get_value(std::string &value, uint64_t key) {
    char buf[2048];
    bool success = logstore_.get(buf, key);
    if (success)
      value = std::string(buf);
  }

  bool remove(const int64_t key) {
    return logstore_.delete_record(key);
  }

 private:
  int64_t start_key_;
  slog::log_store<16384000, 125*1024*1024, slog::udef_kvmap> logstore_;
};

#endif
