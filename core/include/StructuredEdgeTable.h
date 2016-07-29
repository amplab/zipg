#ifndef STRUCTURED_EDGE_TABLE_H
#define STRUCTURED_EDGE_TABLE_H

#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/thread.hpp"

//#include <boost/archive/text_oarchive.hpp>
//#include <boost/archive/text_iarchive.hpp>
//#include <boost/serialization/string.hpp>
//#include <boost/serialization/version.hpp>

class StructuredEdgeTable {
 public:

  StructuredEdgeTable(const std::string edge_file = "")
      : edge_file_(edge_file),
        num_edges_(0) {
  }

  // Reads in the `edge_file_`, convert the data into structured data (i.e.
  // populate the `edges` map).
  void construct();

  void load();

  // Limitation: we assume timestamp for a particular (src, atype) is
  // monotonically increasing for now (think: social network).
  //
  // Thread-safe for concurrent writes.
  void add_assoc(int64_t src, int64_t dst, int64_t atype, int64_t timestamp,
                 const std::string& attr);

  std::vector<SuccinctGraph::Assoc> assoc_range(int64_t src, int64_t atype,
                                                int32_t off, int32_t len);

  int64_t assoc_count(int64_t src, int64_t atype) {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    return edges[std::make_pair(src, atype)].size();
  }

  std::vector<SuccinctGraph::Assoc> assoc_get(
      int64_t src, int64_t atype, const std::set<int64_t>& dst_id_set,
      int64_t t_low, int64_t t_high);

  std::vector<SuccinctGraph::Assoc> assoc_time_range(int64_t src, int64_t atype,
                                                     int64_t t_low,
                                                     int64_t t_high,
                                                     int32_t len);

  void build_backfill_edge_updates(
      std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
      int num_shards_to_mod);

  int num_edges() {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);
    return num_edges_;
  }

//    template<class Archive>
//    void serialize(Archive & ar, const unsigned int version) {
//        // read class state from archive
//        ar & edge_file_;
//        for (auto it = edges.begin(); it != edges.end(); ++it) {
//            int64_t key = it->first;
//            auto& map = it->second;
//            ar & key;
//            for (auto it2 = map.begin(); it2 != map.end(); ++it2) {
//                key = it2->first; // atype
//                auto& vec = it2->second;
//                ar & key;
//                for (auto& edge_data : vec) {
//                    ar & edge_data.dst;
//                    ar & edge_data.timestamp;
//                    ar & edge_data.attr;
//                }
//            }
//        }
//    }

  // LinkBench API
  typedef SuccinctGraph::Assoc Link;

  void getLink(Link& link, int64_t id1, int64_t link_type, int64_t id2);

  void getLinkList(std::vector<Link>& assocs, int64_t id1, int64_t link_type);

  void getLinkList(std::vector<Link>& assocs, int64_t id1, uint64_t link_type,
                   int64_t min_timestamp, int64_t max_timestamp, int64_t offset,
                   int64_t limit);

  bool deleteLink(int64_t id1, int64_t link_type, int64_t id2);
 private:

  typedef Link EdgeData;

  struct EdgeDataComparator {
    bool operator()(const EdgeData& lhs, const EdgeData& rhs) {
      return lhs.time < rhs.time;
    }
  };

  typedef std::set<EdgeData, EdgeDataComparator> EdgeDataSet;
  typedef std::pair<int64_t, int64_t> EdgeRecordId;
  std::unordered_map<EdgeRecordId, EdgeDataSet> edges;

  std::string edge_file_;

  // Protects `edges` and `num_edges_`.
  boost::shared_mutex mutex_;

  int num_edges_;
};

#endif
