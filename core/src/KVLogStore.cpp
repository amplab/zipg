#include "KVLogStore.h"

#include <algorithm>

int64_t KVLogStore::get_value_offset_pos(const int64_t key) {
    long pos = std::lower_bound(
        keys_.begin(), keys_.end(), key) - keys_.begin();
    COND_LOG_E("pos = %d, keys.size = %d\n", pos, keys_.size());
    return (keys_[pos] != key || pos >= keys_.size()) ? -1 : pos;
}

int64_t KVLogStore::get_key_pos(const int64_t value_offset) {
    long pos = std::prev(std::upper_bound(value_offsets_.begin(),
        value_offsets_.end(), value_offset)) - value_offsets_.begin();
    return pos >= value_offsets_.size() ? -1 : pos;
}

int64_t KVLogStore::append(const std::string& value) {
	boost::unique_lock<boost::shared_mutex> lk(mutex_);

    if (tail_ + value.length() > kLogStoreSize) {
        return -1;   // Data exceeds max chunk size
    }

    int64_t start_ts, end_ts;

    start_ts = get_timestamp();
    int64_t key = cur_key_;
    cur_key_++;

    uint64_t end = tail_;
    memcpy(data_ + end, value.c_str(), value.length());

    end_ts = get_timestamp();
    COND_LOG_E("Copied data in %lld us\n", (end_ts - start_ts));

    start_ts = get_timestamp();

    keys_.push_back(key);
    value_offsets_.push_back(end);
    end_ts = get_timestamp();

    COND_LOG_E("Inserted key in %lld us\n", (end_ts - start_ts));


    // Update the index

    start_ts = get_timestamp();

    // min with 0, since data_pos can be small (or zero) initially
	for (int64_t i = std::max(static_cast<int64_t>(0),
			static_cast<int64_t>(end - ngram_n_));
			i < end + value.length() - ngram_n_; i++) {
		uint32_t ngram = Hash::simple_hash3(data_ + i);
        ngram_idx_[ngram].push_back(i);
    }
    tail_ += value.length();

    end_ts = get_timestamp();

    COND_LOG_E("Indexed value in %lld\n", (end_ts - start_ts));

    return key;
}

void KVLogStore::search(
    std::set<int64_t> &_return, const std::string& query)
{
    _return.clear();
    COND_LOG_E("search string '%s' (size %d)\n",
        substring.c_str(), substring.length());

    char *substr = (char *) query.c_str();
	char *suffix = substr + ngram_n_;
	bool skip_filter = (query.length() <= ngram_n_);
	size_t suffix_len = skip_filter ? 0 : query.length() - ngram_n_;
	uint32_t prefix_ngram = Hash::simple_hash3(substr);

    COND_LOG_E("idx sizes: %d, substring '%s'\n", idx_offsets.size(),
        substring_ngram.c_str());

    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    std::vector<uint32_t> idx_off = ngram_idx_[prefix_ngram];
	for (uint32_t i = 0; i < idx_off.size(); i++) {
		if (skip_filter
			|| strncmp(data_ + idx_off[i] + ngram_n_, suffix, suffix_len) == 0) {
			// TODO: Take care of query.length() < ngram_n_ case
			int64_t pos = get_key_pos(idx_off[i]);
			if (pos >= 0)
				_return.insert(keys_[pos]);
		}
	}
}

void KVLogStore::get_value(std::string &value, uint64_t key) {
    value.clear();

    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    int64_t pos = get_value_offset_pos(key);
    COND_LOG_E("get_value_offset_pos done: %lld; key %lld, "
        "value_offsets.size %d\n",
        pos, key, value_offsets_.size());

    if (pos < 0) {
        return;
    }

    int64_t start = value_offsets_[pos];
	int64_t end =
			(pos + 1 < value_offsets_.size()) ? value_offsets_[pos + 1] : tail_;
	size_t len = end - start;

	value.assign(data_ + start, len);
}
