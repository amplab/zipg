#include "KVLogStore.h"

#include <algorithm>

int64_t KVLogStore::append(const std::string& value) {
  boost::unique_lock<boost::shared_mutex> lk(mutex_);

  if (tail_ + value.length() > kLogStoreSize) {
    return -1;   // Data exceeds max chunk size
  }

  int64_t key = cur_key_;
  cur_key_++;

  uint64_t end = tail_;
  memcpy(data_ + end, value.c_str(), value.length());

  k2v[key] = end;
  v2k[end] = key;

  // min with 0, since data_pos can be small (or zero) initially
  for (int64_t i = std::max(static_cast<int64_t>(0),
                            static_cast<int64_t>(end - ngram_n_));
      i < end + value.length() - ngram_n_; i++) {
    uint32_t ngram = Hash::simple_hash3(data_ + i);
    ngram_idx_[ngram].push_back(i);
  }
  tail_ += value.length();

  return key;
}

int64_t KVLogStore::insert(const int64_t key, const std::string& value) {
  boost::unique_lock<boost::shared_mutex> lk(mutex_);

  uint64_t end = tail_;
  memcpy(data_ + end, value.c_str(), value.length());

  COND_LOG_E("[LOGSTORE] K2V mapping (%lld, %llu)\n", key, end);
  COND_LOG_E("[LOGSTORE] V2K mapping (%llu, %lld)\n", end, key);

  k2v[key] = end;
  v2k[end] = key;

  // min with 0, since data_pos can be small (or zero) initially
  for (int64_t i = std::max(static_cast<int64_t>(0),
                            static_cast<int64_t>(end - ngram_n_));
      i < end + value.length() - ngram_n_; i++) {
    uint32_t ngram = Hash::simple_hash3(data_ + i);
    ngram_idx_[ngram].push_back(i);
  }
  tail_ += value.length();

  return key;
}

void KVLogStore::search(std::set<int64_t> &_return, const std::string& query) {
  _return.clear();
  COND_LOG_E("[LOGSTORE] search string '%s' (size %d)\n", query.c_str(),
             query.length());

  char *substr = (char *) query.c_str();
  char *suffix = substr + ngram_n_;
  bool skip_filter = (query.length() <= ngram_n_);
  size_t suffix_len = skip_filter ? 0 : query.length() - ngram_n_;
  uint32_t prefix_ngram = Hash::simple_hash3(substr);

  boost::shared_lock<boost::shared_mutex> lk(mutex_);
  std::vector<uint32_t> idx_off = ngram_idx_[prefix_ngram];

  COND_LOG_E("[LOGSTORE] idx sizes: %d, substring '%s'\n", idx_off.size(),
             substr);
  for (uint32_t i = 0; i < idx_off.size(); i++) {
    if (skip_filter
        || strncmp(data_ + idx_off[i] + ngram_n_, suffix, suffix_len) == 0) {
      // TODO: Take care of query.length() < ngram_n_ case
      BwdMap::iterator it = std::prev(v2k.upper_bound(idx_off[i]));
      _return.insert(it->second);
    }
  }
}

void KVLogStore::get_value(std::string &value, uint64_t key) {
  value.clear();

  boost::shared_lock<boost::shared_mutex> lk(mutex_);

  COND_LOG_E("[LOGSTORE] Get request for key %llu\n", key);

  FwdMap::iterator it = k2v.find(key);
  if (it == k2v.end()) {
    COND_LOG_E("[LOGSTORE] Key not found!\n", key);
    return;
  }

  uint32_t start = it->second;
  uint32_t end = (++it == k2v.end()) ? tail_ : it->second;
  size_t len = end - start;
  COND_LOG_E("[LOGSTORE] start = %u, end = %u, len = %u\n", start, end, len);

  value.assign(data_ + start, len);
}

bool KVLogStore::remove(int64_t key) {
  FwdMap::iterator fwd_it;
  BwdMap::iterator bwd_it;
  {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    fwd_it = k2v.find(key);
    if (fwd_it == k2v.end())
      return false;
    bwd_it = v2k.find(fwd_it->second);
  }

  boost::unique_lock<boost::shared_mutex> lk(mutex_);
  k2v.erase(fwd_it);
  v2k.erase(bwd_it);
  return true;
}
