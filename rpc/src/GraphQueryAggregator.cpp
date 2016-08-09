#include "GraphQueryAggregatorService.h"

#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <mutex>
#include <set>
#include <unordered_map>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>

#include <boost/thread.hpp>
#include <iomanip>
#include <sstream>

#include "graph_shard.h"
#include "ports.h"
#include "utils.h"
#include "async_thread_pool.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

boost::shared_mutex local_shards_data_mutex;
bool local_shards_data_initiated = false;

// a vector of maps: src -> (atype -> [shard id, file offset])
std::vector<
    std::unordered_map<int64_t,
        std::unordered_map<int64_t, std::vector<ThriftEdgeUpdatePtr>> > > edge_update_ptrs;
boost::shared_mutex edge_update_ptrs_mutex;

std::vector<std::unordered_map<int64_t, int32_t>> node_update_ptrs;
boost::shared_mutex node_update_ptrs_mutex;

// anuragk: What we want is per shard control over whether
// the shard is a SuccinctStore shard, SuffixStore shard, or
// a LogStore shard. Right now, for much of the code it is assumed
// that there are dedicated servers for LogStore and SuffixStore,
// although much of it carries forward from Succinct's hardcoded
// NSDI15 implementation.
//
// Ideally, we want to define the number of succict store shards (n_s),
// the number of suffix store shards (n_ss), and the number of log store
// shards (n_ls). Given that the shards are sequentially allocated to different
// hosts, a shard with shard_id would be a succinct store shard if its shard id
// lies between (0, n_s - 1), suffix store if it lies between (n_s, n_s + n_ss - 1),
// etc.

// num_succinctstore_shards_ = total_num_shards
// num_suffixstore_shards_ = 0
// num_logstore_shards_ = 1

class GraphQueryAggregatorServiceHandler :
    virtual public GraphQueryAggregatorServiceIf {

 public:
  GraphQueryAggregatorServiceHandler(
      int total_num_shards, int local_num_shards, int local_host_id,
      const std::vector<std::string>& hostnames,
      const std::vector<AsyncGraphShard*>& local_shards,
      bool multistore_enabled = false, int num_suffixstore_shards = 1,
      int num_logstore_shards = 1)
      : total_num_shards_(total_num_shards),
        local_num_shards_(local_num_shards),
        local_host_id_(local_host_id),
        hostnames_(hostnames),
        local_shards_(local_shards),
        total_num_hosts_(hostnames.size()),
        initiated_(false),
        multistore_enabled_(multistore_enabled),
        num_succinctstore_shards_(total_num_shards),  // FIXME
        num_suffixstore_shards_(num_suffixstore_shards),
        num_logstore_shards_(num_logstore_shards) {
    num_succinctstore_hosts_ = total_num_hosts_;  // FIXME
  }

  // Should just be connection establishment; assumes data loading has already
  // been done.
  int32_t init() {
    if (initiated_) {
      LOG_E("Cluster already initiated\n");
      return 0;
    }
    if (connect_to_aggregators() != 0) {
      LOG_E("Connection to remote aggregators not successful!\n");
      exit(1);
    }

    for (int i = 0; i < total_num_hosts_; ++i) {
      if (i == local_host_id_) {
        continue;
      }
      aggregators_.at(i).connect_to_aggregators();
    }

    LOG_E("Cluster init() done\n");
    initiated_ = true;
    return 0;
  }

  int32_t connect_to_aggregators() {
    aggregators_.clear();

    for (int i = 0; i < hostnames_.size(); ++i) {  // FIXME: total_num_hosts_?
      if (i == local_host_id_) {
        continue;
      }

      COND_LOG_E("Connecting to remote aggregator on host %d...\n", i);
      try {
        shared_ptr<TSocket> socket(
            new TSocket(hostnames_.at(i), QUERY_HANDLER_PORT));
        shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        GraphQueryAggregatorServiceClient client(protocol);

        transport->open();
        COND_LOG_E("Connected!\n");

        aggregators_.insert(
            std::pair<int, GraphQueryAggregatorServiceClient>(i, client));
        aggregator_transports_.push_back(transport);
      } catch (std::exception& e) {
        LOG_E("Could not connect to aggregator %d: %s\n", i, e.what());
        return 1;
      }
    }
    if (hostnames_.size() != total_num_hosts_) {
      LOG_E("%zu total aggregators, but only %zu live\n", total_num_hosts_,
            hostnames_.size());
      return 1;
    }COND_LOG_E("Aggregators connected: cluster has %zu aggregators in total.\n",
        hostnames_.size());
    return 0;
  }

  void shutdown() {
    for (int i = 0; i < total_num_hosts_; ++i) {
      if (i == local_host_id_) {
        continue;
      }
      aggregators_.at(i).disconnect_from_aggregators();
    }
    disconnect_from_aggregators();
  }

  void disconnect_from_aggregators() {
    for (auto transport : aggregator_transports_) {
      if (transport != nullptr && transport->isOpen()) {
        transport->close();
      }
    }
    aggregator_transports_.clear();
  }

  void record_node_append(const int32_t next_shard_id,
                          const int32_t local_shard_id, const int64_t obj) {
    COND_LOG_E(
        "Recording node updates for shard %d at host %d, from shard %d, obj %lld \n",
        local_shard_id, local_host_id_, next_shard_id, obj);

    boost::unique_lock<boost::shared_mutex> lk(node_update_ptrs_mutex);
    auto& map_for_shard = node_update_ptrs.at(
        shard_id_to_shard_idx(local_shard_id));

    map_for_shard[obj] = next_shard_id;

    lk.unlock();
  }

  void record_edge_updates(const int32_t next_shard_id,
                           const int32_t local_shard_id,
                           const std::vector<ThriftSrcAtype> & updates) {
    COND_LOG_E("Recording edge updates for shard %d at host %d, "
        "from shard %d, %lld assoc lists\n",
        local_shard_id, local_host_id_, next_shard_id, updates.size());

    boost::unique_lock<boost::shared_mutex> lk(edge_update_ptrs_mutex);
    ThriftEdgeUpdatePtr ptr;
    auto& map_for_shard = edge_update_ptrs.at(
        shard_id_to_shard_idx(local_shard_id));

    for (auto& update : updates) {
      ptr.shardId = next_shard_id;
      ptr.offset = -1;  // TODO: offset optimization is not implemented yet
      auto& curr_ptrs = map_for_shard[update.src][update.atype];

      // As random edges accumulate in the LogStore and as it sends
      // updates back, it could be that there are many updates from
      // the same store.  If so, record it only once.
      if (curr_ptrs.empty() || curr_ptrs.back().shardId != next_shard_id) {
        curr_ptrs.push_back(ptr);
      }
    }
    lk.unlock();
  }

 public:

  void get_attribute(std::string& _return, const int64_t nodeId,
                     const int32_t attrId) {
    int shard_id = nodeId % total_num_shards_;
    int host_id = shard_id % total_num_hosts_;
    if (host_id == local_host_id_) {
      get_attribute_local(_return, shard_id, nodeId, attrId);
    } else {
      COND_LOG_E("nodeId %lld, host id %d, aggs size\n", nodeId, host_id,
          aggregators_.size());
      aggregators_.at(host_id).get_attribute_local(_return, shard_id, nodeId,
                                                   attrId);
    }
  }

  void get_attribute_local(std::string& _return, const int64_t shard_id,
                           const int64_t node_id, const int32_t attrId) {
    local_shards_.at(shard_id_to_shard_idx(shard_id))->get_attribute_local(
        _return, global_to_local_node_id(node_id, shard_id), attrId);
  }

  void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
    int shard_id = nodeId % total_num_shards_;
    int host_id = shard_id % total_num_hosts_;
    COND_LOG_E("Received: get_neighbors(%lld), route to shard %d on host %d\n",
        nodeId, shard_id, host_id);
    if (host_id == local_host_id_) {
      int shard_idx = shard_id_to_shard_idx(shard_id);
      local_shards_.at(shard_idx)->get_neighbors(_return, nodeId);
    } else {
      aggregators_.at(host_id).get_neighbors_local(_return, shard_id, nodeId);
    }
  }

  void get_neighbors_local(std::vector<int64_t> & _return,
                           const int32_t shardId, const int64_t nodeId) {
    local_shards_[shardId / total_num_hosts_]->get_neighbors(_return, nodeId);
  }

  void get_neighbors_atype(std::vector<int64_t> & _return, const int64_t nodeId,
                           const int64_t atype) {
    int shard_id = nodeId % total_num_shards_;
    int host_id = shard_id % total_num_hosts_;
    if (host_id == local_host_id_) {
      local_shards_.at(shard_id_to_shard_idx(shard_id))->get_neighbors_atype(
          _return, nodeId, atype);
    } else {
      aggregators_.at(host_id).get_neighbors_atype_local(_return, shard_id,
                                                         nodeId, atype);
    }
  }

  void get_neighbors_atype_local(std::vector<int64_t> & _return,
                                 const int32_t shardId, const int64_t nodeId,
                                 const int64_t atype) {
    local_shards_[shardId / total_num_hosts_]->get_neighbors_atype(_return,
                                                                   nodeId,
                                                                   atype);
  }

  void get_edge_attrs(std::vector<std::string> & _return, const int64_t nodeId,
                      const int64_t atype) {
    int shard_id = nodeId % total_num_shards_;
    int host_id = shard_id % total_num_hosts_;
    if (host_id == local_host_id_) {
      local_shards_.at(shard_id_to_shard_idx(shard_id))->get_edge_attrs(_return,
                                                                        nodeId,
                                                                        atype);
    } else {
      aggregators_.at(host_id).get_edge_attrs_local(_return, shard_id, nodeId,
                                                    atype);
    }
  }

  void get_edge_attrs_local(std::vector<std::string> & _return,
                            const int32_t shardId, const int64_t nodeId,
                            const int64_t atype) {
    local_shards_[shardId / total_num_hosts_]->get_edge_attrs(_return, nodeId,
                                                              atype);
  }

  void get_neighbors_attr(std::vector<int64_t> & _return, const int64_t nodeId,
                          const int32_t attrId, const std::string& attrKey) {
    COND_LOG_E("Aggregator get_nhbr_node(nodeId %d, attrId %d)\n", nodeId,
        attrId);

    int shard_id = nodeId % total_num_shards_;
    int host_id = shard_id % total_num_hosts_;
    // Delegate to the shard responsible for nodeId.
    if (host_id == local_host_id_) {
      COND_LOG_E("Delegating to myself\n");

      get_neighbors_attr_local(_return, shard_id, nodeId, attrId, attrKey);
    } else {
      COND_LOG_E("Route to aggregator on host %d\n", host_id);

      aggregators_.at(host_id).get_neighbors_attr_local(_return, shard_id,
                                                        nodeId, attrId,
                                                        attrKey);
    }
  }

  // TODO: rid of shardId? Maybe more expensive to ship an int over network?
  void get_neighbors_attr_local(std::vector<int64_t> & _return,
                                const int32_t shardId, const int64_t nodeId,
                                const int32_t attrId,
                                const std::string& attrKey) {
    COND_LOG_E("In get_nhbr_node_local(shardId %d, nodeId %d, attrId %d)\n",
        shardId, nodeId, attrId);

    std::vector<int64_t> nhbrs;
    get_neighbors_local(nhbrs, shardId, nodeId);
    COND_LOG_E("nhbrs size: %d\n", nhbrs.size());

    // hostId -> [list of responsible nhbr IDs to check]
    std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
    int host_id;

    for (int64_t nhbr_id : nhbrs) {
      host_id = (nhbr_id % total_num_shards_) % total_num_hosts_;
      splits_by_keys[host_id].push_back(nhbr_id);  // global
    }

    for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it) {
      host_id = it->first;
      COND_LOG_E("send target: host %d\n", host_id);
      if (host_id == local_host_id_) {
        filter_nodes_local(_return, it->second, attrId, attrKey);
        COND_LOG_E("locally filtered result: %d\n", _return.size());
      } else {
        COND_LOG_E("host id %d\n", host_id);
        aggregators_.at(host_id).send_filter_nodes_local(it->second, attrId,
                                                         attrKey);
      }
    }

    COND_LOG_E("about to receive from remote hosts\n");

    std::vector<int64_t> shard_result;
    for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it) {
      host_id = it->first;
      COND_LOG_E("recv target: host %d\n", host_id);
      // The equal case has already been computed in loop above
      if (host_id != local_host_id_) {
        aggregators_.at(host_id).recv_filter_nodes_local(shard_result);
        COND_LOG_E("remotely filtered result: %d\n", shard_result.size());
        _return.insert(_return.end(), shard_result.begin(), shard_result.end());
      }
    }
  }

  void filter_nodes_local(std::vector<int64_t>& _return,
                          const std::vector<int64_t>& nodeIds,
                          const int32_t attrId, const std::string& attrKey) {
    COND_LOG_E("in agg. filter_nodes_local, %d ids to filter\n", nodeIds.size());
    // shardId -> [list of responsible nhbr IDs to check]
    std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
    int shard_id;

    for (int64_t nhbr_id : nodeIds) {
      shard_id = nhbr_id % total_num_shards_;
      splits_by_keys[shard_id].push_back(
          global_to_local_node_id(nhbr_id, shard_id));  // to local
    }

    typedef std::future<std::vector<int64_t>> future_t;
    std::unordered_map<int, future_t> futures;
    for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it) {
      COND_LOG_E("sending to shard %d, filter_nodes\n",
          it->first / total_num_hosts_);
      // FIXME?: try to sleep a while? get_nhbr(n, attr) bug here?
      AsyncGraphShard *shard = local_shards_[it->first / total_num_hosts_];
      auto future = shard->async_filter_nodes(it->second, attrId, attrKey);
      futures.insert(
          std::pair<int, future_t>(it->first / total_num_hosts_,
                                   std::move(future)));
      COND_LOG_E("sent");
    }

    _return.clear();
    std::vector<int64_t> shard_result;
    for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it) {
      COND_LOG_E("receiving filter_nodes() result from shard %d, ",
          it->first / total_num_hosts_);
      shard_result = futures[it->first / total_num_hosts_].get();
      COND_LOG_E("size: %d\n", shard_result.size());
      // local back to global
      for (const int64_t local_key : shard_result) {
        // globalKey = localKey * numShards + shardId
        // localKey = (globalKey - shardId) / numShards
        _return.push_back(local_key * total_num_shards_ + it->first);
      }
    }
  }

  void get_nodes(std::set<int64_t> & _return, const int32_t attrId,
                 const std::string& attrKey) {
    for (int i = 0; i < total_num_hosts_; ++i) {
      if (i == local_host_id_) {
        continue;
      }
      aggregators_.at(i).send_get_nodes_local(attrId, attrKey);
    }

    get_nodes_local(_return, attrId, attrKey);

    std::set<int64_t> shard_result;
    for (int i = 0; i < total_num_hosts_; ++i) {
      if (i == local_host_id_) {
        continue;
      }
      aggregators_.at(i).recv_get_nodes_local(shard_result);
      _return.insert(shard_result.begin(), shard_result.end());
    }
  }

  void get_nodes_local(std::set<int64_t> & _return, const int32_t attrId,
                       const std::string& attrKey) {
    typedef std::future<std::set<int64_t>> future_t;
    std::vector<future_t> futures;
    for (auto& shard : local_shards_) {
      auto future = shard->async_get_nodes(attrId, attrKey);
      futures.push_back(std::move(future));
    }

    std::set<int64_t> shard_result;
    _return.clear();

    for (auto& future : futures) {
      shard_result = future.get();
      _return.insert(shard_result.begin(), shard_result.end());
    }
  }

  void get_nodes2(std::set<int64_t> & _return, const int32_t attrId1,
                  const std::string& attrKey1, const int32_t attrId2,
                  const std::string& attrKey2) {
    for (int i = 0; i < total_num_hosts_; ++i) {
      if (i == local_host_id_) {
        continue;
      }
      aggregators_.at(i).send_get_nodes2_local(attrId1, attrKey1, attrId2,
                                               attrKey2);
    }

    get_nodes2_local(_return, attrId1, attrKey1, attrId2, attrKey2);

    std::set<int64_t> shard_result;
    for (int i = 0; i < total_num_hosts_; ++i) {
      if (i == local_host_id_) {
        continue;
      }
      aggregators_.at(i).recv_get_nodes2_local(shard_result);
      _return.insert(shard_result.begin(), shard_result.end());
    }
  }

  void get_nodes2_local(std::set<int64_t> & _return, const int32_t attrId1,
                        const std::string& attrKey1, const int32_t attrId2,
                        const std::string& attrKey2) {
    typedef std::future<std::set<int64_t>> future_t;
    std::vector<future_t> futures;
    for (auto& shard : local_shards_) {
      auto future = shard->async_get_nodes2(attrId1, attrKey1, attrId2,
                                            attrKey2);
      futures.push_back(std::move(future));
    }

    std::set<int64_t> shard_result;
    _return.clear();

    for (auto& future : futures) {
      shard_result = future.get();
      _return.insert(shard_result.begin(), shard_result.end());
    }
  }

  // Used in assoc based queries
  inline void get_edge_update_ptrs(std::vector<ThriftEdgeUpdatePtr>& ptrs,
                                   int shard_idx, int64_t src, int64_t atype) {
    if (!multistore_enabled_) {
      ptrs.clear();
      return;
    }

    COND_LOG_E("Getting edge update pointers at idx=%d, size = %zu\n",
        shard_idx, edge_update_ptrs.size());

    assert(
        shard_idx < edge_update_ptrs.size()
            && "shard_idx >= edge_update_ptrs.size()");

    boost::shared_lock<boost::shared_mutex> lk(edge_update_ptrs_mutex);
    auto& src_map = edge_update_ptrs.at(shard_idx);
    auto src_map_entry = src_map.find(src);
    if (src_map_entry != src_map.end()) {
      auto& atype_map = src_map[src];
      auto atype_map_entry = atype_map.find(atype);
      if (atype_map_entry != atype_map.end()) {
        ptrs = atype_map[atype];
      }
    }

    lk.unlock();
  }

  void assoc_range(std::vector<ThriftAssoc>& _return, int64_t src,
                   int64_t atype, int32_t off, int32_t len) {
    COND_LOG_E("in aggregator assoc_range\n");

    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = src % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      assoc_range_local(_return, shard_id, src, atype, off, len);
    } else {
      COND_LOG_E("assoc_range(src %lld, atype %lld,...) "
          "route to shard %d on host %d",
          src, atype, shard_id, host_id);
      aggregators_.at(host_id).assoc_range_local(_return, shard_id, src, atype,
                                                 off, len);
    }
  }

  // FIXME: the implementation is sequential for now...
  // FIXME: the logic is simply incorrect if off != 0.
  void assoc_range_local(std::vector<ThriftAssoc>& _return, int32_t shardId,
                         int64_t src, int64_t atype, int32_t off, int32_t len) {

    int shard_idx = shard_id_to_shard_idx(shardId);

    COND_LOG_E("assoc_range_local(src %lld, atype %lld, ..., len %d) "
        "shard %d on host %d, shard idx %d of %d shards\n",
        src, atype, len, shardId, local_host_id_, shard_idx,
        local_shards_.size());
    std::vector<ThriftAssoc> assocs;
    int32_t curr_len = 0;
    _return.clear();

    std::vector<ThriftEdgeUpdatePtr> ptrs;
    get_edge_update_ptrs(ptrs, shard_idx, src, atype);
    COND_LOG_E("# update ptrs: %d\n", ptrs.size());
    for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
      if (curr_len >= len) {
        break;
      }

      auto& ptr = *it;
      // int64_t offset = ptr.offset; // TODO: add optimization
      int next_host_id = host_id_for_shard(ptr.shardId);
      if (next_host_id == local_host_id_) {
        int shard_idx_local = shard_id_to_shard_idx(ptr.shardId);
        local_shards_[shard_idx_local]->assoc_range(assocs, src, atype, 0,
                                                    len - curr_len);
      } else {
        aggregators_.at(next_host_id).assoc_range_local(assocs, ptr.shardId,
                                                        src, atype, 0,  // FIXME: this is a hack and potentially expensive
                                                        len - curr_len);
      }
      _return.insert(_return.end(), assocs.begin(), assocs.end());

      curr_len += assocs.size();
    }

    if (_return.size() < len) {
      local_shards_.at(shard_idx)->assoc_range(assocs, src, atype, 0,
                                               len - _return.size());
      COND_LOG_E("local shard returns %d assocs\n", assocs.size());
      _return.insert(_return.end(), assocs.begin(), assocs.end());
    }

    if (!ptrs.empty()) {
      COND_LOG_E("assoc_range_local(%lld, %lld, %d, %d), %d ptrs", src, atype,
          off, len, ptrs.size());
    }

    auto start = _return.begin();
    // Critical to have std::min here, otherwise UB -> segfault
    auto end = _return.begin()
        + std::min(_return.size(), static_cast<size_t>(len));
    COND_LOG_E("about to return, %d assocs before cutoff, ", _return.size());
    _return = std::vector<ThriftAssoc>(start, end);
    COND_LOG_E("%d assocs after\n", _return.size());
  }

  int64_t assoc_count(int64_t src, int64_t atype) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    // %'ing with total_num_shards_ means always going to a primary
    int primary_shard_id = src % total_num_shards_;
    int host_id = primary_shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      return assoc_count_local(primary_shard_id, src, atype);
    } else {
      COND_LOG_E("assoc_count(src %lld, atype %lld) "
          "route to shard %d on host %d, shard idx",
          src, atype, primary_shard_id, host_id);
      return aggregators_.at(host_id).assoc_count_local(primary_shard_id, src,
                                                        atype);
    }
  }

  // This can be called on any Succinct, Suffix, and Log Store machine.
  // Therefore, shardId can be >= num_succinctstore_shards_.
  int64_t assoc_count_local(int32_t shardId, int64_t src, int64_t atype) {
    int shard_idx = shard_id_to_shard_idx(shardId);
    COND_LOG_E("assoc_count_local(src %lld, atype %lld) "
        "shard %d on host %d, shard idx %d",
        src, atype, shardId, local_host_id_, shard_idx);

    std::vector<ThriftEdgeUpdatePtr> ptrs;
    get_edge_update_ptrs(ptrs, shard_idx, src, atype);
    COND_LOG_E("# update ptrs: %d\n", ptrs.size());

    typedef std::future<int64_t> future_t;
    std::vector<future_t> local_futures;

    // Follow all pointers.  Suffix and Log Stores should not have them.
    for (auto& ptr : ptrs) {
      // int64_t offset = ptr.offset; // TODO: add optimization
      int next_host_id = host_id_for_shard(ptr.shardId);
      if (next_host_id == local_host_id_) {
        int shard_idx_local = shard_id_to_shard_idx(ptr.shardId);
        auto future = local_shards_.at(shard_idx_local)->async_assoc_count(
            src, atype);
        local_futures.push_back(std::move(future));
      } else {
        aggregators_.at(next_host_id).send_assoc_count_local(ptr.shardId, src,
                                                             atype);
      }
    }

    // Execute locally
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");
    auto future = local_shards_.at(shard_idx)->async_assoc_count(src, atype);
    local_futures.push_back(std::move(future));

    int64_t cnt = 0;
    for (auto& future : local_futures) {
      cnt += future.get();
    }

    for (auto& ptr : ptrs) {
      int next_host_id = host_id_for_shard(ptr.shardId);
      // We already have all local counts at this point
      if (next_host_id != local_host_id_) {
        cnt += aggregators_.at(next_host_id).recv_assoc_count_local();
      }
    }

    return cnt;
  }

  void assoc_get(std::vector<ThriftAssoc>& _return, const int64_t src,
                 const int64_t atype, const std::set<int64_t>& dstIdSet,
                 const int64_t tLow, const int64_t tHigh) {
    COND_LOG_E("in agg. assoc_get(src %lld, atype %lld)\n", src, atype);
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = src % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      COND_LOG_E("sending to shard %d on localhost\n", shard_id);
      assoc_get_local(_return, shard_id, src, atype, dstIdSet, tLow, tHigh);
    } else {
      COND_LOG_E("sending to shard %d on host %d\n", shard_id, host_id);
      aggregators_.at(host_id).assoc_get_local(_return, shard_id, src, atype,
                                               dstIdSet, tLow, tHigh);
    }
  }

  // FIXME: it currently goes to all shards in parallel, due to lack of times.
  void assoc_get_local(std::vector<ThriftAssoc>& _return, const int32_t shardId,
                       const int64_t src, const int64_t atype,
                       const std::set<int64_t>& dstIdSet, const int64_t tLow,
                       const int64_t tHigh) {
    int shard_idx = shard_id_to_shard_idx(shardId);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E(
        "assoc_get_local(src %lld, atype %lld) "
        "; shardId %d on host %d, shard idx %d, num local shards = %zu\n",
        src, atype, shardId, local_host_id_, shard_idx, local_shards_.size());

    std::vector<ThriftEdgeUpdatePtr> ptrs;
    get_edge_update_ptrs(ptrs, shard_idx, src, atype);

    COND_LOG_E("# update ptrs: %d\n", ptrs.size());

    typedef std::future<std::vector<ThriftAssoc>> future_t;
    std::vector<future_t> local_futures;
    for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
      // int64_t offset = ptr.offset; // TODO: add optimization
      int next_host_id = host_id_for_shard(it->shardId);

      COND_LOG_E("Update ptrs: Next host id = %d\n", next_host_id);
      if (next_host_id == local_host_id_) {
        int shard_idx_local = shard_id_to_shard_idx(it->shardId);
        auto future = local_shards_.at(shard_idx_local)->async_assoc_get(
            src, atype, dstIdSet, tLow, tHigh);
        local_futures.push_back(std::move(future));
      } else {
        aggregators_.at(next_host_id).send_assoc_get_local(it->shardId, src,
                                                           atype, dstIdSet,
                                                           tLow, tHigh);
      }
    }

    COND_LOG_E("Sending assoc_get request to local shard at idx=%d\n",
        shard_idx);

    auto future = local_shards_.at(shard_idx)->async_assoc_get(src, atype,
                                                               dstIdSet, tLow,
                                                               tHigh);
    local_futures.push_back(std::move(future));

    _return.clear();

    for (auto& future : local_futures) {
      std::vector<ThriftAssoc> local_assocs = future.get();
      _return.insert(_return.end(), local_assocs.begin(), local_assocs.end());
    }

    std::vector<ThriftAssoc> assocs;
    for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
      // int64_t offset = ptr.offset; // TODO: add optimization
      int next_host_id = host_id_for_shard(it->shardId);
      COND_LOG_E("Update ptrs: Next host id = %d\n", next_host_id);
      if (next_host_id != local_host_id_) {
        aggregators_.at(next_host_id).recv_assoc_get_local(assocs);
      }
      _return.insert(_return.end(), assocs.begin(), assocs.end());
    }

    COND_LOG_E("assoc_get_local done, returning %d assocs!\n", _return.size());
  }

  void obj_get(std::vector<std::string>& _return, const int64_t nodeId) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = nodeId % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    COND_LOG_E("Received obj_get for nodeId = %lld\n", nodeId);

    if (host_id == local_host_id_) {
      COND_LOG_E("Shard %d is local.\n", shard_id);
      obj_get_local(_return, shard_id, nodeId);
    } else {
      COND_LOG_E("Forwarding to shard %d on host %d.\n", shard_id, host_id);
      aggregators_.at(host_id).obj_get_local(_return, shard_id, nodeId);
    }
  }

  // TODO: multistore logic
  void obj_get_local(std::vector<std::string>& _return, const int32_t shardId,
                     const int64_t nodeId) {
    COND_LOG_E("Received local request for obj_get nodeId = %lld\n", nodeId);
    int shard_idx = shard_id_to_shard_idx(shardId);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    // TODO: Add check for key range to determine if object lies within SuccinctStore shards or LogStore shards
    COND_LOG_E("Shard index = %d, number of shards on this server = %zu\n",
        shard_idx, local_shards_.size());
    local_shards_.at(shard_idx)->obj_get(
        _return, global_to_local_node_id(nodeId, shardId));
  }

  void assoc_time_range(std::vector<ThriftAssoc>& _return, const int64_t src,
                        const int64_t atype, const int64_t tLow,
                        const int64_t tHigh, const int32_t limit) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = src % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      assoc_time_range_local(_return, shard_id, src, atype, tLow, tHigh, limit);
    } else {
      aggregators_.at(host_id).assoc_time_range_local(_return, shard_id, src,
                                                      atype, tLow, tHigh,
                                                      limit);
    }
  }

  void assoc_time_range_local(std::vector<ThriftAssoc>& _return,
                              const int32_t shardId, const int64_t src,
                              const int64_t atype, const int64_t tLow,
                              const int64_t tHigh, const int32_t limit) {
    int shard_idx = shard_id_to_shard_idx(shardId);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E("assoc_time_range_local(src %lld, atype %lld,...) "
        "; shardId %d on host %d, shard idx %d\n",
        src, atype, shardId, local_host_id_, shard_idx);

    std::vector<ThriftEdgeUpdatePtr> ptrs;
    get_edge_update_ptrs(ptrs, shard_idx, src, atype);

    COND_LOG_E("# update ptrs: %d\n", ptrs.size());

    typedef std::future<std::vector<ThriftAssoc>> future_t;
    std::vector<future_t> local_futures;
    for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
      // int64_t offset = ptr.offset; // TODO: add optimization
      int next_host_id = host_id_for_shard(it->shardId);
      if (next_host_id == local_host_id_) {
        int shard_idx_local = shard_id_to_shard_idx(it->shardId);
        auto future = local_shards_.at(shard_idx_local)->async_assoc_time_range(
            src, atype, tLow, tHigh, limit);
        local_futures.push_back(std::move(future));
      } else {
        aggregators_.at(next_host_id).send_assoc_time_range_local(it->shardId,
                                                                  src, atype,
                                                                  tLow, tHigh,
                                                                  limit);
      }
    }

    auto future = local_shards_.at(shard_idx)->async_assoc_time_range(src,
                                                                      atype,
                                                                      tLow,
                                                                      tHigh,
                                                                      limit);
    local_futures.push_back(std::move(future));

    _return.clear();
    for (auto& future : local_futures) {
      std::vector<ThriftAssoc> local_assocs = future.get();
      if (_return.size() + local_assocs.size() <= limit) {
        _return.insert(_return.end(), local_assocs.begin(), local_assocs.end());
      } else {
        _return.insert(
            _return.end(),
            local_assocs.begin(),
            local_assocs.begin()
                + std::min(local_assocs.size(), limit - _return.size()));
      }
    }

    // TODO: Early termination?
    std::vector<ThriftAssoc> assocs;
    for (auto it = ptrs.rbegin(); it != ptrs.rend(); ++it) {
      // int64_t offset = ptr.offset; // TODO: add optimization
      int next_host_id = host_id_for_shard(it->shardId);
      if (next_host_id != local_host_id_) {
        aggregators_.at(next_host_id).recv_assoc_time_range_local(assocs);
      }

      if (_return.size() + assocs.size() <= limit) {
        _return.insert(_return.end(), assocs.begin(), assocs.end());
      } else {
        _return.insert(
            _return.end(), assocs.begin(),
            assocs.begin() + std::min(assocs.size(), limit - _return.size()));
      }
    }

    COND_LOG_E("assoc_time_range done, returning %d assocs (limit %d)!\n",
        _return.size(), limit);
  }

  int64_t obj_add(const std::vector<std::string>& attrs) {
    // Currently on host holding log store shard
    assert(multistore_enabled_ && "multistore not enabled but obj_add called");

    COND_LOG_E("Received obj_add(...)\n");

    int64_t start, end;

    if (local_host_id_ == total_num_hosts_ - 1) {
      COND_LOG_E("Updating local logstore.\n");
      start = get_timestamp();
      int64_t obj = local_shards_.back()->obj_add(attrs);
      end = get_timestamp();

      COND_LOG_E("Updated local logstore in %lld us\n", (end - start));

      start = get_timestamp();
      if (obj != -1) {
        int primary_shard_id = obj % num_succinctstore_shards_;
        int primary_host_id = host_id_for_shard(primary_shard_id);
        // assert(local_host_id_ != primary_host_id); // No loger holds

        COND_LOG_E("Updating host %d, shard %d about obj(%lld)\n",
            primary_host_id, primary_shard_id, obj);

        if (primary_host_id == local_host_id_) {
          record_node_append(
              num_succinctstore_shards_ + num_suffixstore_shards_
                  + num_logstore_shards_ - 1,
              primary_shard_id, obj);
        } else {
          aggregators_.at(primary_host_id).record_node_append(
              num_succinctstore_shards_ + num_suffixstore_shards_
                  + num_logstore_shards_ - 1,
              primary_shard_id, obj);
        }
      }
      end = get_timestamp();
      COND_LOG_E("Updated remote node update pointers in %lld us\n",
          (end - start));
    } else {
      COND_LOG_E("Forwarding assoc_add to host %d\n", (total_num_hosts_ - 1));
      return aggregators_.at(total_num_hosts_ - 1).obj_add(attrs);
    }

    return 0;
  }

  int assoc_add(const int64_t src, const int64_t atype, const int64_t dst,
                const int64_t time, const std::string& attr) {
    assert(
        multistore_enabled_ && "multistore not enabled but assoc_add called");

    COND_LOG_E("Received assoc_add(%lld,%d,%lld,...)\n", src, atype, dst);

    // NOTE: this hard-codes the knowledge that:
    // (1) the last machine is LogStore machine, and
    // (2) its last shard is the append-only store
    if (local_host_id_ == total_num_hosts_ - 1) {

      COND_LOG_E("Updating local logstore.\n");
      int ret = local_shards_.back()->assoc_add(src, atype, dst, time, attr);

      if (!ret) {
        int primary_shard_id = src % num_succinctstore_shards_;
        int primary_host_id = host_id_for_shard(primary_shard_id);
        // assert(local_host_id_ != primary_host_id); // No loger holds

        ThriftSrcAtype src_atype;
        src_atype.src = src;
        src_atype.atype = atype;

        COND_LOG_E("Updating host %d, shard %d about (%lld,%d)\n",
            primary_host_id, primary_shard_id, src, atype);

        if (primary_host_id == local_host_id_) {
          record_edge_updates(
              num_succinctstore_shards_ + num_suffixstore_shards_
                  + num_logstore_shards_ - 1,
              primary_shard_id, { src_atype });
        } else {
          aggregators_.at(primary_host_id).record_edge_updates(
              num_succinctstore_shards_ + num_suffixstore_shards_
                  + num_logstore_shards_ - 1,
              primary_shard_id, { src_atype });
        }
      }

      COND_LOG_E("Finished update!\n");

      return ret;
    } else {
      COND_LOG_E("Forwarding assoc_add to host %d\n", (total_num_hosts_ - 1));
      return aggregators_.at(total_num_hosts_ - 1).assoc_add(src, atype, dst,
                                                             time, attr);
    }
  }

  // LinkBench API
  typedef ThriftAssoc Link;

  void getNodeLocal(std::string& data, int64_t shard_id, int64_t id) {
    data = "";

    try {
      COND_LOG_E("Received local request for getNodeLocal node_id = %lld\n", id);
      int shard_idx = shard_id_to_shard_idx(shard_id);
      assert(
          shard_idx < local_shards_.size()
              && "shard_idx >= local_shards_.size()");

      COND_LOG_E("Shard index = %d, number of shards on this server = %zu\n",
          shard_idx, local_shards_.size());
      int64_t local_id;
      if (local_host_id_ == total_num_hosts_ - 1
          && shard_idx == local_shards_.size() - 1) {
        // This request is for the LogStore shard, don't mess with id.
        local_id = id;
      } else {
        local_id = global_to_local_node_id(id, shard_id);
      }

      local_shards_.at(shard_idx)->getNode(data, local_id);
    } catch (std::exception& e) {
      LOG_E("Exception at getNodeLocal: %s\n", e.what());
    }
  }

  void getNode(std::string& data, int64_t id) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    data = "";

    int shard_id = id % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    COND_LOG_E("Received getNode for nodeId = %lld\n", id);

    if (host_id == local_host_id_) {
      COND_LOG_E("Shard %d is local.\n", shard_id);
      getNodeLocal(data, shard_id, id);
    } else {
      COND_LOG_E("Forwarding to shard %d on host %d.\n", shard_id, host_id);
      try {
        aggregators_.at(host_id).getNodeLocal(data, shard_id, id);
      } catch (std::exception& e) {
        LOG_E("Exception at getNode (first attempt): %s\n", e.what());
      }
    }

    // If the regular lookup did not yield results, search the log store.
    if (data == "") {
      COND_LOG_E("Not found in SuccinctStore, forwarding to LogStore.\n");
      if (host_id == total_num_hosts_ - 1) {
        COND_LOG_E("LogStore shard is local.\n");
        getNodeLocal(data, total_num_shards_, id);
      } else {
        COND_LOG_E("LogStore shard is not local, forwarding to remote host.\n");
        try {
          aggregators_.at(total_num_hosts_ - 1).getNodeLocal(data,
                                                             total_num_shards_,
                                                             id);
        } catch (std::exception& e) {
          LOG_E("Exception at getNode (second attempt): %s\n", e.what());
        }
      }
    }

  }

  int64_t addNode(const int64_t id, const std::string& data) {
    // Currently on host holding log store shard
    assert(multistore_enabled_ && "multistore not enabled but obj_add called");

    COND_LOG_E("Received addNode(%lld, ...)\n", id);

    if (local_host_id_ == total_num_hosts_ - 1) {
      COND_LOG_E("Updating local logstore.\n");
      return local_shards_.back()->addNode(id, data);
    } else {
      COND_LOG_E("Forwarding addNode to host %d\n", (total_num_hosts_ - 1));
      return aggregators_.at(total_num_hosts_ - 1).addNode(id, data);
    }

    return 0;
  }

  bool deleteNodeLocal(int64_t shard_id, int64_t id) {
    COND_LOG_E("Received local request for deleteNodeLocal node_id = %lld\n",
        id);
    int shard_idx = shard_id_to_shard_idx(shard_id);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E("Shard index = %d, number of shards on this server = %zu\n",
        shard_idx, local_shards_.size());
    int64_t local_id;
    if (local_host_id_ == total_num_hosts_ - 1
        && shard_idx == local_shards_.size() - 1) {
      // This request is for the LogStore shard, don't mess with id.
      local_id = id;
    } else {
      local_id = global_to_local_node_id(id, shard_id);
    }

    COND_LOG_E("Final deleteNode request with local_id = %lld\n", local_id);
    return local_shards_.at(shard_idx)->deleteNode(local_id);
  }

  bool deleteNode(int64_t id) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = id % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    COND_LOG_E("Received deleteNode for nodeId = %lld\n", id);

    bool deleted = false;
    if (host_id == local_host_id_) {
      COND_LOG_E("Shard %d is local.\n", shard_id);
      deleted = deleteNodeLocal(shard_id, id);
    } else {
      COND_LOG_E("Forwarding to shard %d on host %d.\n", shard_id, host_id);
      deleted = aggregators_.at(host_id).deleteNodeLocal(shard_id, id);
    }

    // If the regular lookup did not yield results, search the log store.
    if (!deleted) {
      COND_LOG_E("Not found in SuccinctStore, forwarding to LogStore.\n");
      if (host_id == total_num_hosts_ - 1) {
        COND_LOG_E("LogStore shard is local.\n");
        return deleteNodeLocal(total_num_shards_, id);
      } else {
        COND_LOG_E("LogStore shard is not local, forwarding to remote host.\n");
        aggregators_.at(total_num_hosts_ - 1).deleteNodeLocal(total_num_shards_,
                                                              id);
      }
    }

    return deleted;
  }

  bool updateNode(const int64_t id, const std::string& data) {
    bool deleted = deleteNode(id);
    if (deleted) {
      int64_t ret = addNode(id, data);
      assert(ret == id);
      return true;
    }
    return false;
  }

  void getLinkLocal(Link& link, int64_t shard_id, const int64_t id1,
                    const int64_t link_type, const int64_t id2) {

    int shard_idx = shard_id_to_shard_idx(shard_id);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E("getLinkLocal(src %lld, atype %lld, dst %lld)\n", id1, link_type,
        id2);

    // First try designated shard
    bool found = local_shards_.at(shard_idx)->getLink(link, id1, link_type,
                                                      id2);
    if (!found) {
      COND_LOG_E(
          "Edge not found in SuccinctStore, perhaps it exists in the LogStore.\n");
      std::vector<ThriftEdgeUpdatePtr> ptrs;
      get_edge_update_ptrs(ptrs, shard_idx, id1, link_type);

      COND_LOG_E("# update ptrs: %d\n", ptrs.size());
      if (!ptrs.empty()) {
        COND_LOG_E(
            "Update ptrs present for edge, checking if LogStore has requested edge.");
        assert(ptrs.size() == 1);
        ThriftEdgeUpdatePtr ptr = ptrs.back();
        int next_host_id = host_id_for_shard(ptr.shardId);
        if (next_host_id == local_host_id_) {
          int shard_idx_local = shard_id_to_shard_idx(ptr.shardId);
          COND_LOG_E("LogStore is local at shard idx=%lld\n", shard_idx_local);
          local_shards_.at(shard_idx_local)->getLink(link, id1, link_type, id2);
        } else {
          COND_LOG_E("LogStore is remote at host id = %lld, shard id=%lld\n",
              next_host_id, ptr.shardId);
          aggregators_.at(next_host_id).getLinkLocal(link, ptr.shardId, id1,
                                                     link_type, id2);
        }
      }
    }

    COND_LOG_E("getLink done!\n");
  }

  void getLink(Link& link, const int64_t id1, const int64_t link_type,
               const int64_t id2) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = id1 % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      getLinkLocal(link, shard_id, id1, link_type, id2);
    } else {
      aggregators_.at(host_id).getLinkLocal(link, shard_id, id1, link_type,
                                            id2);
    }
  }

  bool addLink(const Link& link) {
    assert(
        multistore_enabled_ && "multistore not enabled but assoc_add called");

    COND_LOG_E("Received addLink(%lld,%d,%lld)\n", link.srcId, link.atype,
        link.dstId);

    // NOTE: this hard-codes the knowledge that:
    // (1) the last machine is LogStore machine, and
    // (2) its last shard is the append-only store
    if (local_host_id_ == total_num_hosts_ - 1) {
      COND_LOG_E("Updating local logstore.\n");
      bool added = local_shards_.back()->addLink(link);

      if (added) {
        int primary_shard_id = link.srcId % num_succinctstore_shards_;
        int primary_host_id = host_id_for_shard(primary_shard_id);
        // assert(local_host_id_ != primary_host_id); // No loger holds

        ThriftSrcAtype src_atype;
        src_atype.src = link.srcId;
        src_atype.atype = link.atype;

        int32_t logstore_shard_id = total_num_shards_;
        COND_LOG_E(
            "Adding update ptr to shard %lld for edge-record identified by (id1=%lld, link_type=%lld) at primary shard %lld, host %lld\n",
            logstore_shard_id, link.srcId, link.atype, primary_shard_id,
            primary_host_id);

        if (primary_host_id == local_host_id_) {
          record_edge_updates(logstore_shard_id, primary_shard_id,
                              { src_atype });
        } else {
          aggregators_.at(primary_host_id).record_edge_updates(
              logstore_shard_id, primary_shard_id, { src_atype });
        }
      }

      return added;

      COND_LOG_E("Finished update!\n");
    } else {
      COND_LOG_E("Forwarding assoc_add to host %d\n", (total_num_hosts_ - 1));
      return aggregators_.at(total_num_hosts_ - 1).addLink(link);
    }
  }

  bool deleteLinkLocal(const int64_t shard_id, const int64_t id1,
                       const int64_t link_type, const int64_t id2) {
    int shard_idx = shard_id_to_shard_idx(shard_id);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E("deleteLinkLocal(src %lld, atype %lld, dst %lld)\n", id1,
        link_type, id2);

    // First try designated shard
    bool deleted = local_shards_.at(shard_idx)->deleteLink(id1, link_type, id2);
    if (!deleted) {
      std::vector<ThriftEdgeUpdatePtr> ptrs;
      get_edge_update_ptrs(ptrs, shard_idx, id1, link_type);

      COND_LOG_E("# update ptrs: %d\n", ptrs.size());
      if (!ptrs.empty()) {
        assert(ptrs.size() == 1);
        ThriftEdgeUpdatePtr ptr = ptrs.back();
        int next_host_id = host_id_for_shard(ptr.shardId);
        if (next_host_id == local_host_id_) {
          int shard_idx_local = shard_id_to_shard_idx(ptr.shardId);
          deleted = local_shards_.at(shard_idx_local)->deleteLink(id1,
                                                                  link_type,
                                                                  id2);
        } else {
          deleted = aggregators_.at(next_host_id).deleteLinkLocal(ptr.shardId,
                                                                  id1,
                                                                  link_type,
                                                                  id2);
        }
      }
    }

    COND_LOG_E("deleteLink done!\n");
    return deleted;
  }

  bool deleteLink(const int64_t id1, const int64_t link_type,
                  const int64_t id2) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    int shard_id = id1 % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      return deleteLinkLocal(shard_id, id1, link_type, id2);
    } else {
      return aggregators_.at(host_id).deleteLinkLocal(shard_id, id1, link_type,
                                                      id2);
    }
  }

  bool updateLink(const Link& link) {
    bool deleted = deleteLink(link.srcId, link.atype, link.dstId);
    bool added = false;
    if (deleted) {
      added = addLink(link);
    }

    return deleted && added;
  }

  void getLinkListLocal(std::vector<Link>& assocs, const int64_t shard_id,
                        const int64_t id1, const int64_t link_type) {

    int shard_idx = shard_id_to_shard_idx(shard_id);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E(
        "Received getLinkListLocal(shard_id=%lld, id=%lld, link_type=%lld) request.\n",
        shard_id, id1, link_type);

    // get update ptr; there should only be one
    std::vector<ThriftEdgeUpdatePtr> ptrs;
    get_edge_update_ptrs(ptrs, shard_idx, id1, link_type);
    COND_LOG_E("# update ptrs: %d\n", ptrs.size());

    typedef std::future<std::vector<ThriftAssoc>> future_t;
    future_t update_future;
    int update_host_id;

    if (!ptrs.empty()) {
      ThriftEdgeUpdatePtr ptr = ptrs.back();

      COND_LOG_E("Forwarding request to LogStore shard in parallel.\n");

      // First send out request to LogStore shard.
      update_host_id = host_id_for_shard(ptr.shardId);
      if (update_host_id == local_host_id_) {
        int shard_idx_local = shard_id_to_shard_idx(ptr.shardId);
        COND_LOG_E("LogStore shard is local at index = %lld.\n",
            shard_idx_local);
        update_future = local_shards_.at(shard_idx_local)->async_getLinkList(
            id1, link_type);
      } else {
        COND_LOG_E("LogStore shard is remote at host = %lld, shard_id = %lld\n",
            update_host_id, ptr.shardId);
        aggregators_.at(update_host_id).send_getLinkListLocal(ptr.shardId, id1,
                                                              link_type);
      }
    }

    // Then process query at designated shard.
    COND_LOG_E("Processing query at designated shard.\n");
    local_shards_.at(shard_idx)->getLinkList(assocs, id1, link_type);

    // Finally process the responce from LogStore shard.
    if (!ptrs.empty()) {
      COND_LOG_E("Receiving response from LogStore shard in parallel.\n");

      std::vector<ThriftAssoc> update_assocs;
      if (update_host_id == local_host_id_) {
        update_assocs = update_future.get();
      } else {
        aggregators_.at(update_host_id).recv_getLinkListLocal(update_assocs);
      }

      // Add responses from LogStore to result
      COND_LOG_E("Adding response from LogStore shard to result.\n");
      assocs.insert(assocs.begin(), update_assocs.begin(), update_assocs.end());
    }

    COND_LOG_E("getLinkListLocal done, returning %d links!\n", assocs.size());

  }

  void getLinkList(std::vector<Link>& assocs, const int64_t id1,
                   const int64_t link_type) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    COND_LOG_E("Received getLinkList(id1=%lld, link_type=%lld) request\n", id1,
        link_type);

    int shard_id = id1 % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      COND_LOG_E("Forwarding to local shard %lld\n", shard_id);
      getLinkListLocal(assocs, shard_id, id1, link_type);
    } else {
      COND_LOG_E("Forwarding to remote shard %lld on host %lld\n", shard_id,
          host_id);
      aggregators_.at(host_id).getLinkListLocal(assocs, shard_id, id1,
                                                link_type);
    }
  }

  void getFilteredLinkListLocal(std::vector<Link>& assocs,
                                const int64_t shard_id, const int64_t id1,
                                const int64_t link_type,
                                const int64_t min_timestamp,
                                const int64_t max_timestamp,
                                const int64_t offset, const int64_t limit) {

    int shard_idx = shard_id_to_shard_idx(shard_id);
    assert(
        shard_idx < local_shards_.size()
            && "shard_idx >= local_shards_.size()");

    COND_LOG_E(
        "Received getFilteredLinkListLocal(shard_id=%lld, id=%lld, link_type=%lld, min_timestamp=%lld, max_timesamp=%lld, offset=%lld, limit=%lld) request.\n",
        shard_id, id1, link_type, min_timestamp, max_timestamp, offset, limit);

    // get update ptr; there should only be one
    std::vector<ThriftEdgeUpdatePtr> ptrs;
    get_edge_update_ptrs(ptrs, shard_idx, id1, link_type);
    COND_LOG_E("# update ptrs: %d\n", ptrs.size());

    typedef std::future<std::vector<ThriftAssoc>> future_t;
    future_t update_future;
    int update_host_id;

    if (!ptrs.empty()) {
      ThriftEdgeUpdatePtr ptr = ptrs.back();

      COND_LOG_E("Forwarding request to LogStore shard in parallel.\n");

      // First send out request to LogStore shard.
      update_host_id = host_id_for_shard(ptr.shardId);
      if (update_host_id == local_host_id_) {
        int shard_idx_local = shard_id_to_shard_idx(ptr.shardId);
        COND_LOG_E("LogStore shard is local at index = %lld.\n",
            shard_idx_local);
        update_future = local_shards_.at(shard_idx_local)
            ->async_getFilteredLinkList(id1, link_type, min_timestamp,
                                        max_timestamp, offset, limit);
      } else {
        COND_LOG_E("LogStore shard is remote at host = %lld, shard_id = %lld\n",
            update_host_id, ptr.shardId);
        aggregators_.at(update_host_id).send_getFilteredLinkListLocal(
            ptr.shardId, id1, link_type, min_timestamp, max_timestamp, offset,
            limit);
      }
    }

    // Then process query at designated shard.
    COND_LOG_E("Processing query at designated shard.\n");
    local_shards_.at(shard_idx)->getFilteredLinkList(assocs, id1, link_type,
                                                     min_timestamp,
                                                     max_timestamp, offset,
                                                     limit);

    // Finally process the responce from LogStore shard.
    if (!ptrs.empty()) {
      COND_LOG_E("Receiving response from LogStore shard in parallel.\n");
      std::vector<ThriftAssoc> update_assocs;
      if (update_host_id == local_host_id_) {
        update_assocs = update_future.get();
      } else {
        aggregators_.at(update_host_id).recv_getFilteredLinkListLocal(
            update_assocs);
      }

      // Add responses from LogStore to result
      COND_LOG_E("Adding response from LogStore shard to result.\n");
      if (assocs.size() < limit) {
        size_t deficit = limit - assocs.size();
        std::vector<ThriftAssoc>::iterator begin, end;
        begin = update_assocs.begin();
        if (update_assocs.size() < deficit) {
          end = update_assocs.end();
        } else {
          end = update_assocs.begin() + deficit;
        }

        assocs.insert(assocs.begin(), begin, end);
      }
    }

    COND_LOG_E("getFilteredLinkListLocal done, returning %d links!\n",
        assocs.size());

  }

  void getFilteredLinkList(std::vector<Link>& assocs, const int64_t id1,
                           const int64_t link_type, const int64_t min_timestamp,
                           const int64_t max_timestamp, const int64_t offset,
                           const int64_t limit) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");

    COND_LOG_E(
        "Received getFilteredLinkList(id1=%lld, link_type=%lld, min_timestamp=%lld, max_timestamp=%lld, offset=%lld, limit=%lld) request\n",
        id1, link_type, min_timestamp, max_timestamp, offset, limit);

    int shard_id = id1 % total_num_shards_;
    int host_id = shard_id % num_succinctstore_hosts_;

    if (host_id == local_host_id_) {
      COND_LOG_E("Forwarding to local shard %lld\n", shard_id);
      getFilteredLinkListLocal(assocs, shard_id, id1, link_type, min_timestamp,
                               max_timestamp, offset, limit);
    } else {
      COND_LOG_E("Forwarding to remote shard %lld on host %lld\n", shard_id,
          host_id);
      aggregators_.at(host_id).getFilteredLinkListLocal(assocs, shard_id, id1,
                                                        link_type,
                                                        min_timestamp,
                                                        max_timestamp, offset,
                                                        limit);
    }
  }

  int64_t countLinks(int64_t id1, int64_t link_type) {
    return assoc_count(id1, link_type);
  }

 private:

// globalKey = localKey * numShards + shardId
// localKey = (globalKey - shardId) / numShards
  inline int64_t global_to_local_node_id(int64_t global_node_id, int shard_id) {
    assert(total_num_shards_ > 0 && "total_num_shards_ <= 0");
    return (global_node_id - shard_id) / total_num_shards_;
  }

// Host 0 to n 1: the SuccinctStores, hash-partitioned
// Host n - 1: the empty LogStore
  inline int host_id_for_shard(int shard_id) {
    if (!multistore_enabled_ || shard_id < num_succinctstore_shards_) {
      assert(num_succinctstore_hosts_ > 0 && "num_succinctstore_hosts_ <= 0");
      return shard_id % num_succinctstore_hosts_;
    }
    // FIXME
    COND_LOG_E("LogStore shard %d resides on host %d\n", shard_id,
        num_succinctstore_hosts_ - 1);
    return num_succinctstore_hosts_ - 1;
  }

// Limitation: this assumes 1 LogStore machine.
  inline int shard_id_to_shard_idx(int shard_id) {
    if (!multistore_enabled_) {
      assert(total_num_hosts_ > 0 && "total_num_hosts_ <= 0");
      return shard_id / total_num_hosts_;
    }

    COND_LOG_E("Converting shard id %d to shard idx\n", shard_id);
    int diff = shard_id - total_num_shards_;
    if (diff >= 0) {
      COND_LOG_E(
          "Shard id %d >= number of SuccinctStore shards %d, returning LogStore shard id.\n",
          shard_id, num_succinctstore_shards_);
      return local_shards_.size() - 1;  // log store
    }
    return shard_id / num_succinctstore_hosts_;  // succinct st., round-robin
  }

// Limitation: this assumes 1 SuffixStore machine and 1 LogStore machine.
  inline int shard_idx_to_shard_id(int shard_idx) {
    if (local_host_id_ < num_succinctstore_hosts_
        && shard_idx < local_num_shards_) {
      return shard_idx * num_succinctstore_hosts_ + local_host_id_;
    } else {
      // case: log store machine
      assert(local_host_id_ == num_succinctstore_hosts_ - 1);
      return shard_idx + num_succinctstore_shards_;
    }
  }

  const int total_num_shards_;  // total # of logical shards
  const int local_num_shards_;

// id of this physical node
  const int local_host_id_;
// all aggregators in the cluster, hostnames used for opening sockets
  std::vector<std::string> hostnames_;
  const int total_num_hosts_;
  bool initiated_, multistore_enabled_;

  int num_succinctstore_hosts_;

  int num_succinctstore_shards_;
  int num_suffixstore_shards_;
  int num_logstore_shards_;

  std::vector<AsyncGraphShard*> local_shards_;

// Maps host id to aggregator handle.  Does not contain self.
  std::unordered_map<int, GraphQueryAggregatorServiceClient> aggregators_;
  std::vector<shared_ptr<TTransport>> aggregator_transports_;

};

// Dummy factory that just delegates fields.
class ProcessorFactory : public TProcessorFactory {
 public:
  ProcessorFactory(int total_num_shards, int local_num_shards,
                   int local_host_id, const std::vector<std::string>& hostnames,
                   bool multistore_enabled, int num_suffixstore_shards,
                   int num_logstore_shards,
                   const std::vector<AsyncGraphShard*>& shards)
      : total_num_shards_(total_num_shards),
        local_num_shards_(local_num_shards),
        local_host_id_(local_host_id),
        hostnames_(hostnames),
        multistore_enabled_(multistore_enabled),
        num_suffixstore_shards_(num_suffixstore_shards),
        num_logstore_shards_(num_logstore_shards),
        shards_(shards) {
  }

  boost::shared_ptr<TProcessor> getProcessor(const TConnectionInfo&) {
    boost::shared_ptr<GraphQueryAggregatorServiceHandler> handler(
        new GraphQueryAggregatorServiceHandler(total_num_shards_,
                                               local_num_shards_,
                                               local_host_id_, hostnames_,
                                               shards_, multistore_enabled_,
                                               num_suffixstore_shards_,
                                               num_logstore_shards_));
    boost::shared_ptr<TProcessor> handlerProcessor(
        new GraphQueryAggregatorServiceProcessor(handler));
    return handlerProcessor;
  }

 private:
  int total_num_shards_;
  int local_num_shards_;
  int local_host_id_;
  const std::vector<std::string>& hostnames_;
  const std::vector<AsyncGraphShard*> shards_;
  bool multistore_enabled_;
  int num_suffixstore_shards_, num_logstore_shards_;
};

void print_usage(char *exec) {
  LOG_E("Usage: %s [-t total_num_shards] [-s local_num_shards] "
        "[-h hostsfile] [-i local_host_id]\n",
        exec);
}

std::string node_part_name(std::string base_path, int32_t shard_id,
                           int32_t num_shards) {
  std::string max = std::to_string(num_shards);
  std::stringstream node_part;
  node_part << base_path << "-part" << std::setfill('0')
            << std::setw(max.length()) << shard_id << "of" << max << "WithPtrs";
  return node_part.str();
}

std::string edge_part_name(std::string base_path, int32_t shard_id,
                           int32_t num_shards) {
  std::string max = std::to_string(num_shards);
  std::stringstream edge_part;
  edge_part << base_path << "-part" << std::setfill('0')
            << std::setw(max.length()) << shard_id << "of" << max;
  return edge_part.str();
}

void handler(int sig) {
  void *array[10];
  size_t size = backtrace(array, 10);

  LOG_E("Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

int main(int argc, char **argv) {
  if (argc < 2) {
    print_usage(argv[0]);
    return -1;
  }

  signal(SIGSEGV, handler);

  LOG_E("Launched aggregator with command line: ");
  for (int i = 0; i < argc; i++) {
    LOG_E("%s ", argv[i]);
  }
  LOG_E("\n");

  int c, total_num_shards, local_num_shards, local_host_id;
  bool multistore_enabled;
  int num_suffixstore_shards, num_logstore_shards;
  int sa_sampling_rate = 32, isa_sampling_rate = 64, npa_sampling_rate = 128;
  std::string hostsfile;
  while ((c = getopt(argc, argv, "t:s:i:h:f:l:m:x:y:z:")) != -1) {
    switch (c) {
      case 't':
        total_num_shards = atoi(optarg);
        break;
      case 's':
        local_num_shards = atoi(optarg);
        break;
      case 'i':
        local_host_id = atoi(optarg);
        break;
      case 'h':
        hostsfile = optarg;
        break;
      case 'f':
        num_suffixstore_shards = atoi(optarg);
        break;
      case 'l':
        num_logstore_shards = atoi(optarg);
        break;
      case 'm':
        multistore_enabled = (std::string(optarg) == "T");
        break;
      case 'x':
        sa_sampling_rate = atoi(optarg);
        break;
      case 'y':
        isa_sampling_rate = atoi(optarg);
        break;
      case 'z':
        npa_sampling_rate = atoi(optarg);
        break;
      default:
        LOG_E("Could not parse command line arguments.\n")
        ;
    }
  }

  std::ifstream hosts(hostsfile);
  std::string host;
  std::vector<std::string> hostnames;
  while (std::getline(hosts, host)) {
    hostnames.push_back(host);
  }

  int total_num_hosts = hostnames.size();

  std::string node_file = std::string(argv[optind]);
  std::string edge_file = std::string(argv[optind + 1]);

  std::vector<AsyncGraphShard*> local_shards;

  local_shards.resize(local_num_shards);
  std::vector<std::thread> init_threads;
  unsigned num_threads = std::thread::hardware_concurrency();
  num_threads = num_threads == 0 ? 64 : num_threads;
  LOG_E("Setting concurrency to %u\n", num_threads);
  AsyncThreadPool *pool = new AsyncThreadPool(num_threads);
  LOG_E("Total number of hosts = %d, local host id = %d\n", total_num_hosts,
        local_host_id);
  for (size_t i = 0; i < local_num_shards; i++) {
    int shard_id = i * total_num_hosts + local_host_id;
    std::string node_filename, edge_filename;
    node_filename = node_part_name(node_file, shard_id, total_num_shards);
    edge_filename = edge_part_name(edge_file, shard_id, total_num_shards);
    LOG_E("Shard Id = %d, Node File = %s, Edge File = %s\n", shard_id,
          node_filename.c_str(), edge_filename.c_str());
    init_threads.push_back(
        std::thread(
            [i, node_filename, edge_filename, sa_sampling_rate, isa_sampling_rate, npa_sampling_rate, shard_id, total_num_shards, num_suffixstore_shards, num_logstore_shards, &pool, &local_shards] {
              local_shards[i] = new AsyncGraphShard(node_filename, edge_filename,
                  false, sa_sampling_rate,
                  isa_sampling_rate,
                  npa_sampling_rate, shard_id,
                  total_num_shards,
                  StoreMode::SuccinctStore,
                  num_suffixstore_shards,
                  num_logstore_shards, pool);
            }));
  }

  std::for_each(init_threads.begin(), init_threads.end(), [](std::thread &t) {
    t.join();
  });

  if (local_host_id == hostnames.size() - 1) {
    // LogStore
    int shard_id = total_num_shards;
    LOG_E("Shard Id = %d, Log Store", shard_id);
    AsyncGraphShard *shard = new AsyncGraphShard("", "", false,
                                                 sa_sampling_rate,
                                                 isa_sampling_rate,
                                                 npa_sampling_rate, shard_id,
                                                 total_num_shards,
                                                 StoreMode::LogStore,
                                                 num_suffixstore_shards,
                                                 num_logstore_shards, pool);
    local_shards.push_back(shard);

    // +1 because of the last, empty shard
    edge_update_ptrs.resize(total_num_shards + num_logstore_shards + 1);
    node_update_ptrs.resize(total_num_shards + num_logstore_shards + 1);
    LOG_E("[LOGSTORE] Have %zu update pointer tables.\n",
          edge_update_ptrs.size());
  } else {
    // Succ.
    edge_update_ptrs.resize(total_num_shards);
    node_update_ptrs.resize(total_num_shards);
    LOG_E("[SUCCINCT] Have %zu update pointer tables.\n",
          edge_update_ptrs.size());
  }

  LOG_E("Handler started\n");

  int port = QUERY_HANDLER_PORT;
  try {
    shared_ptr<ProcessorFactory> processor_factory(
        new ProcessorFactory(total_num_shards, local_num_shards, local_host_id,
                             hostnames, multistore_enabled,
                             num_suffixstore_shards, num_logstore_shards,
                             local_shards));
    shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transport_factory(
        new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());

    // Note: 1st arg being a processor factory is essential in supporting
    // multiple clients (e.g. in throughput benchmarks).
    TThreadedServer server(processor_factory, server_transport,
                           transport_factory, protocol_factory);

    server.serve();
  } catch (std::exception& e) {
    LOG_E("Exception at GraphQueryAggregator:main(): %s\n", e.what());
  }
  return 0;
}
