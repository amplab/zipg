#ifndef EDGELOGSTORE_H_
#define EDGELOGSTORE_H_

#include <atomic>

#include "slog/splitordered/hashtable.h"
#include "slog/faclog.h"

struct edge_info {
  int64_t link_type;
  int64_t id2;
  int64_t timestamp;
  std::string data;
  std::atomic<bool> valid;

  edge_info() {
    valid.store(true);
    link_type = -1;
    id2 = -1;
    timestamp = -1;
  }

  edge_info(int64_t link_type, int64_t id2, int64_t timestamp,
            const std::string& data) {
    valid.store(true);
    this->link_type = link_type;
    this->id2 = id2;
    this->timestamp = timestamp;
    this->data = data;
  }

  edge_info(const edge_info& other) {
    valid.store(other.valid.load());
    link_type = other.link_type;
    id2 = other.id2;
    timestamp = other.timestamp;
    data = other.data;
  }

  edge_info& edge_info::operator=(const edge_info& other) {
    valid.store(other.valid.load());
    link_type = other.link_type;
    id2 = other.id2;
    timestamp = other.timestamp;
    data = other.data;
    return this;
  }
};

class EdgeLogStore {
 public:
  typedef slog::faclog_consistent<edge_info> edge_record;
  typedef SuccinctGraph::Assoc Link;

  struct LinkComparator {
    bool operator()(const Link& lhs, const Link& rhs) {
      return lhs.time < rhs.time;
    }
  };

  EdgeLogStore() {
  }

  // LinkBench API
  bool addLink(int64_t id1, int64_t link_type, int64_t id2, int64_t timestamp,
               const std::string& data) {
    uint32_t edge_record_idx;
    if (edge_record_index_.get(id1, &edge_record_idx)) {
      return insertLink(edge_record_idx, link_type, id2, timestamp, data);
    }

    uint32_t idx = edge_records_.advance_back();
    edge_record_index_.insert(id1, idx);
    return insertLink(idx, link_type, id2, timestamp, data);
  }

  bool getLink(Link& link, int64_t id1, int64_t link_type, int64_t id2) {
    auto res = findLink(id1, link_type, id2);

    if (res.first >= 0 && res.second >= 0) {
      edge_info& info = edge_records_[res.first][res.second];
      link.src_id = id1;
      link.dst_id = id2;
      link.atype = link_type;
      link.time = info.timestamp;
      link.attr = info.data;
      return true;
    }

    return false;
  }

  void getLinkList(std::vector<Link>& assocs, int64_t id1, int64_t link_type) {
    uint32_t edge_record_idx;
    if (edge_record_index_.get(id1, &edge_record_idx)) {
      // Scan through the actual edge record
      edge_record& rec = edge_records_[edge_record_idx];
      uint32_t size = rec.size();
      for (uint32_t i = 0; i < size; i++) {
        if (rec[i].valid.load()) {
          assocs.emplace_back(id1, rec[i].id2, rec[i].link_type,
                              rec[i].timestamp, rec[i].data);
        }
      }
    }
  }

  int64_t countLinks(int64_t id1, int64_t link_type) {
    uint32_t edge_record_idx;
    int64_t count = 0;
    if (edge_record_index_.get(id1, &edge_record_idx)) {
      // Scan through the actual edge record
      edge_record& rec = edge_records_[edge_record_idx];
      uint32_t size = rec.size();
      for (uint32_t i = 0; i < size; i++) {
        if (rec[i].valid.load()) {
          count++;
        }
      }
    }
    return count;
  }

  void getLinkList(std::vector<Link>& assocs, int64_t id1, uint64_t link_type,
                   int64_t min_timestamp, int64_t max_timestamp, int64_t offset,
                   int64_t limit) {
    uint32_t edge_record_idx;
    std::set<Link, LinkComparator> filtered_links;
    if (edge_record_index_.get(id1, &edge_record_idx)) {
      // Scan through the actual edge record
      edge_record& rec = edge_records_[edge_record_idx];
      uint32_t size = rec.size();
      for (uint32_t i = 0; i < size; i++) {
        if (rec[i].valid.load() && rec[i].timestamp > min_timestamp
            && rec[i].timestamp < max_timestamp) {
          filtered_links.emplace(id1, rec[i].id2, rec[i].link_type,
                                 rec[i].timestamp, rec[i].data);
        }
      }
    }

    std::set<Link, LinkComparator>::iterator it = filtered_links.begin();
    while (offset-- && it != filtered_links.end()) {
      it++;
    }

    for (; it != filtered_links.end() && assocs.size() <= limit; it++) {
      assocs.push_back(*it);
    }
  }

  bool deleteLink(int64_t id1, int64_t link_type, int64_t id2) {
    auto res = findLink(id1, link_type, id2);

    if (res.first >= 0 && res.second >= 0) {
      edge_records_[res.first][res.second].valid.store(false);
    }

    return false;
  }

 private:
  std::pair<int64_t, int64_t> findLink(int64_t id1, int64_t link_type,
                                       int64_t id2) {
    uint32_t edge_record_idx;
    if (edge_record_index_.get(id1, &edge_record_idx)) {
      // Scan through the actual edge record
      edge_record& rec = edge_records_[edge_record_idx];
      uint32_t size = rec.size();
      for (uint32_t i = 0; i < size; i++) {
        if (rec[i].link_type == link_type && rec[i].id2 == id2
            && rec[i].valid.load()) {
          return std::pair<int64_t, int64_t>(edge_record_idx, i);
        }
      }
    }
    return std::pair<int64_t, int64_t>(-1, -1);
  }

  bool insertLink(uint32_t edge_record_idx, int64_t link_type, int64_t id2,
                  int64_t timestamp, const std::string& data) {
    while (1) {
      // Scan through the actual edge record
      edge_record& rec = edge_records_[edge_record_idx];
      uint32_t size = rec.size();
      for (uint32_t i = 0; i < size; i++) {
        if (rec[i].link_type == link_type && rec[i].id2 == id2
            && rec[i].valid.load()) {
          return false;
        }
      }
      edge_info new_info(link_type, id2, timestamp, data);
      if (edge_records_[edge_record_idx].grow(size, new_info)) {
        return true;
      }
    }
  }

  // All the data structures we will need
  splitordered::hash_table<uint32_t> edge_record_index_;
  slog::faclog_consistent<edge_record> edge_records_;
};

#endif /* EDGELOGSTORE_H_ */
