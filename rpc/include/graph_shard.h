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
#include "async_thread_pool.h"

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
    assert(store_mode_ == StoreMode::LogStore);

    COND_LOG_E("Handling assoc_add(%lld,%d,%lld,%lld...)",
        src, atype, dst, time);

    // Note the argument order is switched
    int ret = graph_log_store_->append_edge(src, dst, atype, time, attr);

    COND_LOG_E("; ret = %d\n", ret);
    return ret;
  }

  int64_t obj_add(const std::vector<std::string>& attrs) {
    assert(store_mode_ == StoreMode::LogStore);

    // Note the argument order is switched
    return graph_log_store_->append_node(attrs);
  }

  // LinkBench API
  typedef ThriftAssoc Link;

  bool getNode(std::string& data, const int64_t id) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("getNode on SuccinctStore shard.\n");
        return graph_->getNode(data, id);
      case StoreMode::LogStore:
        COND_LOG_E("getNode on LogStore shard.\n");
        return graph_log_store_->getNode(data, id);
    }

    return false;
  }

  int64_t addNode(const int64_t id, const std::string& data) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        assert(false && "Cannot add node to SuccinctStore!");
        return -1;
      case StoreMode::LogStore:
        COND_LOG_E("addNode on LogStore shard.\n");
        return graph_log_store_->addNode(id, data);
    }
    return -1;
  }

  bool deleteNode(int64_t id) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("deleteNode on SuccinctStore shard.\n");
        return graph_->deleteNode(id);
      case StoreMode::LogStore:
        COND_LOG_E("deleteNode on LogStore shard.\n");
        return graph_log_store_->deleteNode(id);
    }
    return false;
  }

  bool getLink(Link& link, const int64_t id1, const int64_t link_type,
               const int64_t id2) {
    SuccinctGraph::Assoc _link;
    bool found = false;
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("getLink on SuccinctStore shard.\n");
        found = graph_->getLink(_link, id1, link_type, id2);
        break;
      case StoreMode::LogStore:
        COND_LOG_E("getLink on LogStore shard.\n");
        found = graph_log_store_->getLink(_link, id1, link_type, id2);
        break;
    }

    if (found) {
      COND_LOG_E("Found link\n");
      link.__set_srcId(_link.src_id);
      link.__set_dstId(_link.dst_id);
      link.__set_atype(_link.atype);
      link.__set_timestamp(_link.time);
      link.__set_attr(_link.attr);
    }

    return found;
  }

  bool addLink(const Link& link) {
    SuccinctGraph::Assoc _link = { link.srcId, link.dstId, link.atype, link
        .timestamp, link.attr };

    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        assert(false && "Cannot add link to SuccinctStore!");
        return false;
      case StoreMode::LogStore:
        COND_LOG_E("addLink on LogStore shard.\n");
        return graph_log_store_->addLink(_link);
    }

    return false;
  }

  bool deleteLink(const int64_t id1, const int64_t link_type,
                  const int64_t id2) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("deleteLink on SuccinctStore shard.\n");
        return graph_->deleteLink(id1, link_type, id2);
      case StoreMode::LogStore:
        COND_LOG_E("deleteLink on LogStore shard.\n");
        return graph_log_store_->deleteLink(id1, link_type, id2);
    }
    return false;
  }

  void getLinkList(std::vector<Link>& assocs, const int64_t id1,
                   const int64_t link_type) {
    std::vector<SuccinctGraph::Assoc> _links;
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("getLinkList on SuccinctStore shard.\n");
        graph_->getLinkList(_links, id1, link_type);
        break;
      case StoreMode::LogStore:
        COND_LOG_E("getLinkList on LogStore shard.\n");
        graph_log_store_->getLinkList(_links, id1, link_type);
        break;
    }

    assocs.resize(_links.size());
    size_t i = 0;
    for (auto _link : _links) {
      assocs[i].__set_srcId(_link.src_id);
      assocs[i].__set_dstId(_link.dst_id);
      assocs[i].__set_atype(_link.atype);
      assocs[i].__set_timestamp(_link.time);
      assocs[i].__set_attr(_link.attr);
      ++i;
    }
  }

  void getFilteredLinkList(std::vector<Link>& assocs, const int64_t id1,
                           const int64_t link_type, const int64_t min_timestamp,
                           const int64_t max_timestamp, const int64_t offset,
                           const int64_t limit) {
    std::vector<SuccinctGraph::Assoc> _links;
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("getLinkList(...) on SuccinctStore shard.\n");
        graph_->getLinkList(_links, id1, link_type, min_timestamp,
                            max_timestamp, offset, limit);
        break;
      case StoreMode::LogStore:
        COND_LOG_E("getLinkList(...) on LogStore shard.\n");
        graph_log_store_->getLinkList(_links, id1, link_type, min_timestamp,
                                      max_timestamp, offset, limit);
        break;
    }

    assocs.resize(_links.size());
    size_t i = 0;
    for (auto _link : _links) {
      assocs[i].__set_srcId(_link.src_id);
      assocs[i].__set_dstId(_link.dst_id);
      assocs[i].__set_atype(_link.atype);
      assocs[i].__set_timestamp(_link.time);
      assocs[i].__set_attr(_link.attr);
      ++i;
    }
  }

  void init_rpq_ctx(int64_t label, SuccinctGraph::RPQContext& ctx) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("init_rpq_ctx(...) on SuccinctStore shard.\n");
        graph_->init_rpq_ctx(label, ctx);
        break;
      case StoreMode::LogStore:
        // Do nothing;
        break;
    }
  }

  void advance_rpq_ctx(SuccinctGraph::RPQContext& ret, int64_t label,
                       const SuccinctGraph::RPQContext& ctx) {
    switch (store_mode_) {
      case StoreMode::SuccinctStore:
        COND_LOG_E("advance_rpq_ctx(...) on SuccinctStore shard.\n");
        graph_->advance_rpq_ctx(ret, label, ctx);
        break;
      case StoreMode::LogStore:
        // Do nothing;
        break;
    }
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
 public:
  AsyncGraphShard(const std::string& node_file, const std::string& edge_file,
                  bool construct, int32_t sa_sampling_rate,
                  int32_t isa_sampling_rate, int32_t npa_sampling_rate,
                  int shard_id, int total_num_shards,
                  const StoreMode store_mode, int num_suffixstore_shards,
                  int num_logstore_shards, AsyncThreadPool* pool)
      : GraphShard(node_file, edge_file, construct, sa_sampling_rate,
                   isa_sampling_rate, npa_sampling_rate, shard_id,
                   total_num_shards, store_mode, num_suffixstore_shards,
                   num_logstore_shards) {
    pool_ = pool;
  }

  // Async functions using futures
  std::future<std::vector<int64_t>> async_filter_nodes(
      const std::vector<int64_t> & nodeIds, const int32_t attrId,
      const std::string& attrKey) {
    return pool_->enqueue([&] {
      std::vector<int64_t> res;
      filter_nodes(res, nodeIds, attrId, attrKey);
      return res;
    });
  }

  std::future<std::set<int64_t>> async_get_nodes(const int32_t attrId,
                                                 const std::string& attrKey) {
    return pool_->enqueue([&] {
      std::set<int64_t> res;
      get_nodes(res, attrId, attrKey);
      return res;
    });
  }

  std::future<std::set<int64_t>> async_get_nodes2(const int32_t attrId1,
                                                  const std::string& attrKey1,
                                                  const int32_t attrId2,
                                                  const std::string& attrKey2) {
    return pool_->enqueue([&] {
      std::set<int64_t> res;
      get_nodes2(res, attrId1, attrKey1, attrId2, attrKey2);
      return res;
    });
  }

  std::future<int64_t> async_assoc_count(int64_t src, int64_t atype) {
    return pool_->enqueue([&] {
      return assoc_count(src, atype);
    });
  }

  std::future<std::vector<int64_t>> async_get_neighbors_atype(
      const int64_t node_id, const int64_t atype) {

    return pool_->enqueue([&] {
      std::vector<int64_t> nhbrs;
      get_neighbors_atype(nhbrs, node_id, atype);
      return nhbrs;
    });

  }

  std::future<std::vector<ThriftAssoc>> async_assoc_get(
      const int64_t src, const int64_t atype, const std::set<int64_t>& dstIdSet,
      const int64_t tLow, const int64_t tHigh) {
    return pool_->enqueue([&] {
      std::vector<ThriftAssoc> res;
      assoc_get(res, src, atype, dstIdSet, tLow, tHigh);
      return res;
    });
  }

  std::future<std::vector<ThriftAssoc>> async_assoc_time_range(
      const int64_t src, const int64_t atype, const int64_t tLow,
      const int64_t tHigh, const int32_t limit) {
    return pool_->enqueue([&] {
      std::vector<ThriftAssoc> res;
      assoc_time_range(res, src, atype, tLow, tHigh, limit);
      return res;
    });
  }

  std::future<std::vector<ThriftAssoc>> async_getLinkList(
      const int64_t id1, const int64_t link_type) {
    return pool_->enqueue([&] {
      std::vector<ThriftAssoc> res;
      getLinkList(res, id1, link_type);
      return res;
    });
  }

  std::future<std::vector<ThriftAssoc>> async_getFilteredLinkList(
      const int64_t id1, const int64_t link_type, const int64_t min_timestamp,
      const int64_t max_timestamp, const int64_t offset, const int64_t limit) {
    return pool_->enqueue(
        [&] {
          std::vector<ThriftAssoc> res;
          getFilteredLinkList(res, id1, link_type, min_timestamp, max_timestamp, offset, limit);
          return res;
        });
  }

  std::future<SuccinctGraph::RPQContext> async_init_rpq_ctx(int64_t label) {
    return pool_->enqueue([&] {
      SuccinctGraph::RPQContext ctx;
      init_rpq_ctx(label, ctx);
      return ctx;
    });
  }

  std::future<SuccinctGraph::RPQContext> async_advance_rpq_ctx(
      int64_t label, const SuccinctGraph::RPQContext& ctx) {
    return pool_->enqueue([&] {
      SuccinctGraph::RPQContext ret;
      advance_rpq_ctx(ret, label, ctx);
      return ret;
    });
  }

  // TODO: Add more async functions
 private:
  AsyncThreadPool *pool_;
};

#endif
