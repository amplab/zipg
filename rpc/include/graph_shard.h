#ifndef GRAPHSHARD_H_
#define GRAPHSHARD_H_

#include "GraphFormatter.hpp"
#include "GraphLogStore.h"
#include "GraphSuffixStore.h"
#include "SuccinctGraph.hpp"
#include "utils.h"

#include <set>
#include <vector>
#include <future>

class GraphShard {
 public:
  GraphShard(const std::string& node_file, const std::string& edge_file,
             bool construct, int32_t sa_sampling_rate,
             int32_t isa_sampling_rate, int32_t npa_sampling_rate, int shard_id,
             int total_num_shards, const StoreMode store_mode,
             int num_suffixstore_shards, int num_logstore_shards)
      : shard_id_(shard_id),
        total_num_shards_(total_num_shards),
        node_file_(node_file),
        edge_file_(edge_file),
        construct_(construct),
        store_mode_(store_mode),
        num_suffixstore_shards_(num_suffixstore_shards),
        num_logstore_shards_(num_logstore_shards) {

    if (construct) {
      node_table_empty_ = !file_or_dir_exists(node_file);
      edge_table_empty_ = !file_or_dir_exists(edge_file);
    } else {
      std::string suffix;
      switch (store_mode_) {
        case StoreMode::SuccinctStore:
          suffix = ".succinct";
          break;
        case StoreMode::SuffixStore:
          suffix = "_suffixstore";
          break;
        case StoreMode::LogStore:
          suffix = "_logstore";
          break;
      }
      node_table_empty_ = !file_or_dir_exists(node_file + suffix);
      edge_table_empty_ = !file_or_dir_exists(edge_file + suffix);
    }

    LOG_E("shard id %d, total num shards %d; isa %d, sa %d, npa %d"
          " (specified, please check actual)\n",
          shard_id_, total_num_shards_, isa_sampling_rate, sa_sampling_rate,
          npa_sampling_rate);
    LOG_E("Store mode: %d\n", store_mode);

    switch (store_mode_) {
      case StoreMode::SuccinctStore: {
        graph_ = new SuccinctGraph("");
        graph_->set_npa_sampling_rate(npa_sampling_rate);
        graph_->set_sa_sampling_rate(sa_sampling_rate);
        graph_->set_isa_sampling_rate(isa_sampling_rate);
        if (construct_) {
          LOG_E("Construct is set to true: starting to construct & encode\n");
          if (!node_table_empty_ && !edge_table_empty_) {
            graph_->construct(node_file_, edge_file_);  // in parallel
          } else if (!node_table_empty_) {
            graph_->construct_node_table(node_file_);
          } else if (!edge_table_empty_) {
            graph_->construct_edge_table(edge_file_);
          } else {
            assert(false && "Neither node file nor edge file exists!");
          }
        } else {
          LOG_E("Construct is set to false: starting to load\n");
          if (!node_table_empty_ && !edge_table_empty_) {
            graph_->load(node_file_, edge_file_);
          } else if (!node_table_empty_) {
            graph_->load_node_table(node_file_);
          } else if (!edge_table_empty_) {
            graph_->load_edge_table(edge_file_);
          } else {
            assert(false && "Neither node file nor edge file exists!");
          }
        }
        break;
      }

      case StoreMode::SuffixStore: {
        graph_suffix_store_ = new GraphSuffixStore(node_file_, edge_file_);
        if (construct_) {
          graph_suffix_store_->construct();
        } else {
          graph_suffix_store_->load();
        }
        break;
      }

      case StoreMode::LogStore: {
        graph_log_store_ = new GraphLogStore(node_file_, edge_file_);

        if (shard_id_ == total_num_shards_) {
          // This process is the append-only, initially empty store
          // the node file and edge file will be ignored, since
          // construct() / load() is not called
          break;
        }

        if (construct_) {
          graph_log_store_->construct();
        } else {
          graph_log_store_->load();
        }
        break;
      }

      default: {
        LOG_E("[FATAL] Unsupported store mode.\n");
        exit(-1);
      }
    }
    LOG_E("Initialization at this shard: done\n");
  }

  // In principle, nodeId should be in this shard's edge table.
  void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
    // Your implementation goes here
    COND_LOG_E("Received: get_neighbors(%lld)\n", nodeId);

    assert(nodeId % total_num_shards_ == shard_id_);
    if (edge_table_empty_) {
      _return.clear();
      return;
    }
    graph_->get_neighbors(_return, nodeId);
  }

  void get_neighbors_atype(std::vector<int64_t> & _return, const int64_t nodeId,
                           const int64_t atype) {
    COND_LOG_E("get_neighbors_atype\n");

    assert(nodeId % total_num_shards_ == shard_id_);
    if (edge_table_empty_) {
      _return.clear();
      return;
    }
    graph_->get_neighbors(_return, nodeId, atype);
  }

  void get_edge_attrs(std::vector<std::string> & _return, const int64_t nodeId,
                      const int64_t atype) {
    COND_LOG_E("get_edge_attrs\n");
    assert(nodeId % total_num_shards_ == shard_id_);
    if (edge_table_empty_) {
      _return.clear();
      return;
    }
    graph_->get_edge_attrs(_return, nodeId, atype);
  }

  void get_nodes(std::set<int64_t> & _return, const int32_t attrId,
                 const std::string& attrKey) {
    COND_LOG_E("get_nodes\n");

    _return.clear();
    if (node_table_empty_) {
      return;
    }

    std::set<int64_t> local_keys;
    graph_->get_nodes(local_keys, attrId, attrKey);

    // TODO: this assumes a particular form of hash partitioning
    auto it = _return.begin();
    for (int64_t local_key : local_keys) {
      it = _return.insert(it, local_key * total_num_shards_ + shard_id_);
    }
  }

  void get_nodes2(std::set<int64_t> & _return, const int32_t attrId1,
                  const std::string& attrKey1, const int32_t attrId2,
                  const std::string& attrKey2) {
    COND_LOG_E("get_nodes2\n");

    _return.clear();
    if (node_table_empty_) {
      return;
    }

    std::set<int64_t> local_keys;
    graph_->get_nodes(local_keys, attrId1, attrKey1, attrId2, attrKey2);

    // TODO: this assumes a particular form of hash partitioning
    auto it = _return.begin();
    for (int64_t local_key : local_keys) {
      it = _return.insert(it, local_key * total_num_shards_ + shard_id_);
    }
  }

  void get_attribute_local(std::string& _return, const int64_t nodeId,
                           const int32_t attrId) {
    graph_->get_attribute(_return, nodeId, attrId);
  }

  void filter_nodes(std::vector<int64_t> & _return,
                    const std::vector<int64_t> & nodeIds, const int32_t attrId,
                    const std::string& attrKey) {
    COND_LOG_E("filter_nodes received\n");
    graph_->filter_nodes(_return, nodeIds, attrId, attrKey);
  }

  void assoc_range(std::vector<ThriftAssoc>& _return, int64_t src,
                   int64_t atype, int32_t off, int32_t len) {
    std::vector<SuccinctGraph::Assoc> vec;
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        vec = std::move(graph_->assoc_range(src, atype, off, len));
        break;
      case StoreMode::SuffixStore:
        vec = std::move(graph_suffix_store_->assoc_range(src, atype, off, len));
        break;
      case StoreMode::LogStore:
        vec = std::move(graph_log_store_->assoc_range(src, atype, off, len));
        break;
    }

    // TODO: any better way?
    // NB: the fields are Thrift-generated, so this may not be portable.
    _return.clear();
    _return.resize(vec.size());
    size_t i = 0;
    for (const auto& assoc : vec) {
      _return[i].srcId = assoc.src_id;
      _return[i].dstId = assoc.dst_id;
      _return[i].atype = assoc.atype;
      _return[i].timestamp = assoc.time;
      // should be the same as 'lhs = std::move(rhs)'
      _return[i].attr.assign(std::move(assoc.attr));
      ++i;
    }
  }

  int64_t assoc_count(int64_t src, int64_t atype) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        return graph_->assoc_count(src, atype);
      case StoreMode::SuffixStore:
        return graph_suffix_store_->assoc_count(src, atype);
      case StoreMode::LogStore:
        return graph_log_store_->assoc_count(src, atype);
    }
    return 0;
  }

  void assoc_get(std::vector<ThriftAssoc>& _return, const int64_t src,
                 const int64_t atype, const std::set<int64_t>& dstIdSet,
                 const int64_t tLow, const int64_t tHigh) {
    std::vector<SuccinctGraph::Assoc> vec;
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("in shard assoc_get, about to call graph\n");
        vec = std::move(graph_->assoc_get(src, atype, dstIdSet, tLow, tHigh));
        COND_LOG_E("done: in shard assoc_get, about to call graph\n");
        break;

      case StoreMode::SuffixStore:
        vec = std::move(
            graph_suffix_store_->assoc_get(src, atype, dstIdSet, tLow, tHigh));
        break;

      case StoreMode::LogStore:
        vec = std::move(
            graph_log_store_->assoc_get(src, atype, dstIdSet, tLow, tHigh));
        break;
    }

    // TODO: any better way?
    // NB: the fields are Thrift-generated, so this may not be portable.
    _return.clear();
    _return.resize(vec.size());
    size_t i = 0;
    for (const auto& assoc : vec) {
      _return[i].srcId = assoc.src_id;
      _return[i].dstId = assoc.dst_id;
      _return[i].atype = assoc.atype;
      _return[i].timestamp = assoc.time;
      // should be the same as 'lhs = std::move(rhs)'
      _return[i].attr.assign(std::move(assoc.attr));
      ++i;
    }
  }

  void obj_get(std::vector<std::string>& _return, const int64_t local_id) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        graph_->obj_get(_return, local_id);
        break;

      case StoreMode::SuffixStore:
        graph_suffix_store_->obj_get(_return, local_id);
        break;

      case StoreMode::LogStore:
        graph_log_store_->obj_get(_return, local_id);
        break;
    }
  }

  void assoc_time_range(std::vector<ThriftAssoc>& _return, const int64_t src,
                        const int64_t atype, const int64_t tLow,
                        const int64_t tHigh, const int32_t limit) {
    std::vector<SuccinctGraph::Assoc> vec;
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        vec = std::move(
            graph_->assoc_time_range(src, atype, tLow, tHigh, limit));
        break;

      case StoreMode::SuffixStore:
        vec = std::move(
            graph_suffix_store_->assoc_time_range(src, atype, tLow, tHigh,
                                                  limit));
        break;

      case StoreMode::LogStore:
        vec = std::move(
            graph_log_store_->assoc_time_range(src, atype, tLow, tHigh, limit));
        break;
    }

    // TODO: any better way?
    // NB: the fields are Thrift-generated, so this may not be portable.
    _return.clear();
    _return.resize(vec.size());
    size_t i = 0;
    for (const auto& assoc : vec) {
      _return[i].srcId = assoc.src_id;
      _return[i].dstId = assoc.dst_id;
      _return[i].atype = assoc.atype;
      _return[i].timestamp = assoc.time;
      // should be the same as 'lhs = std::move(rhs)'
      _return[i].attr.assign(std::move(assoc.attr));
      ++i;
    }
  }

  int assoc_add(const int64_t src, const int64_t atype, const int64_t dst,
                const int64_t time, const std::string& attr) {
    assert(store_mode_ == StoreMode::LogStore);COND_LOG_E("Handling assoc_add(%lld,%d,%lld,%lld...)",
        src, atype, dst, time);

    // Note the argument order is switched
    int ret = graph_log_store_->append_edge(src, dst, atype, time, attr);

    COND_LOG_E("; ret = %d\n", ret);
    return ret;
  }

  int64_t obj_add(const std::vector<std::string>& attrs) {
    assert(store_mode_ == StoreMode::LogStore);

    // Note the argument order is switched
    int64_t start, end;
    start = get_timestamp();
    int64_t ret = graph_log_store_->append_node(attrs);
    end = get_timestamp();
    COND_LOG_E("obj_add: Time taken at server = %lld\n", (end - start));
    return ret;
  }

 private:

  // By default, StoreMode::SuccinctStore
  const StoreMode store_mode_;

  const int shard_id_;
  const int total_num_shards_;

  const std::string node_file_;
  const std::string edge_file_;
  const bool construct_;

  bool node_table_empty_ = true;
  bool edge_table_empty_ = true;

  const int num_suffixstore_shards_, num_logstore_shards_;

  SuccinctGraph *graph_ = nullptr;
  GraphLogStore *graph_log_store_ = nullptr;
  GraphSuffixStore *graph_suffix_store_ = nullptr;

};

class AsyncGraphShard : public GraphShard {

  AsyncGraphShard(const std::string& node_file, const std::string& edge_file,
                  bool construct, int32_t sa_sampling_rate,
                  int32_t isa_sampling_rate, int32_t npa_sampling_rate,
                  int shard_id, int total_num_shards,
                  const StoreMode store_mode, int num_suffixstore_shards,
                  int num_logstore_shards)
      : GraphShard(node_file, edge_file, construct, sa_sampling_rate,
                   isa_sampling_rate, npa_sampling_rate, shard_id,
                   total_num_shards, store_mode, num_suffixstore_shards,
                   num_logstore_shards) {
  }

  // Async functions using futures
  std::future<std::vector<int64_t>> async_filter_nodes(
      const std::vector<int64_t> & nodeIds, const int32_t attrId,
      const std::string& attrKey) {
    return std::async(std::launch::async, [&] {
      std::vector<int64_t> res;
      filter_nodes(res, nodeIds, attrId, attrKey);
      return res;
    });
  }

  std::future<std::set<int64_t>> async_get_nodes(const int32_t attrId,
                                                 const std::string& attrKey) {
    return std::async(std::launch::async, [&] {
      std::set<int64_t> res;
      get_nodes(res, attrId, attrKey);
      return res;
    });
  }

  std::future<std::set<int64_t>> async_get_nodes2(const int32_t attrId1,
                                                  const std::string& attrKey1,
                                                  const int32_t attrId2,
                                                  const std::string& attrKey2) {
    return std::async(std::launch::async, [&] {
      std::set<int64_t> res;
      get_nodes2(res, attrId1, attrKey1, attrId2, attrKey2);
      return res;
    });
  }

  std::future<int64_t> async_assoc_count(int64_t src, int64_t atype) {
    return std::async(std::launch::async, [&] {
      return assoc_count(src, atype);
    });
  }

  std::future<std::vector<ThriftAssoc>> async_assoc_get(
      const int64_t src, const int64_t atype, const std::set<int64_t>& dstIdSet,
      const int64_t tLow, const int64_t tHigh) {
    return std::async(std::launch::async, [&] {
      std::vector<ThriftAssoc> res;
      assoc_get(res, src, atype, dstIdSet, tLow, tHigh);
      return res;
    });
  }

  std::future<std::vector<ThriftAssoc>> async_assoc_time_range(
      const int64_t src, const int64_t atype, const int64_t tLow,
      const int64_t tHigh, const int32_t limit) {
    return std::async(std::launch::async, [&] {
      std::vector<ThriftAssoc> res;
      assoc_time_range(res, src, atype, tLow, tHigh, limit);
      return res;
    });
  }

  // TODO: Add more async functions
};

#endif
