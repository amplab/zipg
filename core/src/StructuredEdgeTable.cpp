#include "StructuredEdgeTable.h"

#include "GraphFormatter.hpp"
#include "utils.h"

#include <algorithm>

constexpr char SERDE_DELIM = '\x02';

void StructuredEdgeTable::construct() {
  // Do nothing
}

void StructuredEdgeTable::load() {
  // Do nothing
}

void StructuredEdgeTable::add_assoc(int64_t src, int64_t dst, int64_t atype,
                                    int64_t timestamp,
                                    const std::string& attr) {
  boost::unique_lock<boost::shared_mutex> lock(mutex_);

  edges[std::make_pair(src, atype)].insert(EdgeData { src, dst, atype,
      timestamp, attr });
  ++num_edges_;
}

std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_range(
    int64_t src, int64_t atype, int32_t off, int32_t len) {
  COND_LOG_E("GraphLogStore assoc_range(src = %lld, atype = %lld, off = %d, len = %d)\n",
      src, atype, off, len);

  assert(false && "Should not be here.");
  std::vector<SuccinctGraph::Assoc> assocs;
  return assocs;
}

// FIXME: scan for now...
std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_get(
    int64_t src, int64_t atype, const std::set<int64_t>& dst_id_set,
    int64_t t_low, int64_t t_high) {
  COND_LOG_E("GraphLogStore assoc_get(src = %" PRId64 ", atype = %" PRId64 ","
      " dstIdSet = ..., tLow = %" PRId64 ", tHigh = %" PRId64 ")\n",
      src, atype, t_low, t_high);

  assert(false && "Should not be here.");
  std::vector<SuccinctGraph::Assoc> assocs;
  return assocs;
}

// FIXME: scan for now...
std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_time_range(
    int64_t src, int64_t atype, int64_t t_low, int64_t t_high, int32_t len) {
  COND_LOG_E("GraphLogStore assoc_time_range(src = %lld, atype = %lld, tLow = %lld, "
      "tHigh = %lld, len = %d)\n",
      src, atype, t_low, t_high, len);

  std::vector<SuccinctGraph::Assoc> assocs;
  assert(false && "Should not be here.");
  std::vector<SuccinctGraph::Assoc> assocs;
  return assocs;
}

void StructuredEdgeTable::build_backfill_edge_updates(
    std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
    int num_shards_to_mod) {

  LOG_E("StructuredEdgeTable::build_backfill_edge_updates: %d shards\n",
        edge_updates.size());
}

// LinkBench API
void StructuredEdgeTable::getLink(Link& link, int64_t id1, int64_t link_type,
                                  int64_t id2) {
  boost::shared_lock<boost::shared_mutex> lk(mutex_);
  for (EdgeData edge_data : edges[std::make_pair(id1, link_type)]) {
    if (edge_data.dst_id == id2) {
      link = edge_data;
      return;
    }
  }
}

void StructuredEdgeTable::getLinkList(std::vector<Link>& assocs, int64_t id1,
                                      int64_t link_type) {
  boost::shared_lock<boost::shared_mutex> lk(mutex_);
  for (EdgeData edge_data : edges[std::make_pair(id1, link_type)]) {
    assocs.push_back(edge_data);
  }
}

void StructuredEdgeTable::getLinkList(std::vector<Link>& assocs, int64_t id1,
                                      uint64_t link_type, int64_t min_timestamp,
                                      int64_t max_timestamp, int64_t offset,
                                      int64_t limit) {
  EdgeData min, max;
  min.src_id = max.src_id = id1;
  min.atype = max.atype = link_type;
  min.time = min_timestamp;
  max.time = max_timestamp;
  EdgeDataSet edge_set;
  {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    edge_set = edges[std::make_pair(id1, link_type)];
  }
  EdgeDataSet::iterator begin = edge_set.lower_bound(min);
  EdgeDataSet::iterator end = edge_set.upper_bound(max);
  if (begin + offset == edge_set.end()) {
    return;
  }

  for (EdgeDataSet::iterator it = begin + offset;
      it != end && assocs.size() <= limit; it++) {
    if (it->time > max_timestamp)
      break;
    assocs.push_back(*it);
  }
}

bool StructuredEdgeTable::deleteLink(int64_t id1, int64_t link_type,
                                     int64_t id2) {
  boost::shared_lock<boost::shared_mutex> lk(mutex_);
  EdgeDataSet::iterator it = edges[std::make_pair(id1, link_type)];
  bool deleted = false;
  while (it != edges[std::make_pair(id1, link_type)].end()) {
    EdgeDataSet::iterator current = it++;
    if (current->dst_id == id2) {
      edges[std::make_pair(id1, link_type)].erase(current);
      deleted = true;
    }
  }
  return deleted;
}
