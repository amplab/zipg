#ifndef GRAPH_LOG_STORE_H
#define GRAPH_LOG_STORE_H

#include "FileLogStoreEdgeTable.h"
#include "GraphFormatter.hpp"
#include "KVLogStore.h"
#include "StructuredEdgeTable.h"

#include <set>
#include <string>
#include <unordered_map>
#include <vector>

// A graph that is backed by LogStore and thus supports individual node/edge
// insertions.  In other words, the Node Table is backed by KVLogStore, and the
// Edge Table by FileLogStore.
//
// Limitation: it relies on an ngram index to perform search on the node
// properties.  By default, n = 3, and therefore we can't search along an empty
// search string (we could if n is smaller).
//
// Locking is not done here and only delegated to the underlying Succinct
// classes.
class GraphLogStore {
public:

    // The `node_file` here is just the un-delimited node properties (the
    // values file).  The keys are assumed to be 0-based line numbers; the
    // key-value pointers are computed by using newlines as record delimiters.
    //
    // The `edge_file` is the unformatted edge list.
    GraphLogStore(
        const std::string& node_file,
        const std::string& edge_file)
        : node_file_(node_file),
          edge_file_(edge_file),
          node_pointer_file(""), // FIXME?
          edge_table_(edge_file),
          max_num_edges_(3500000) // FIXME: hard-coded
    { }

    void construct();
    void load();

    // TODO: think about where this key should come from; and locking.
    // Limitation: `node_id` must be larger than all current node_id's managed
    // by the current GraphLogStore (because insertion sort is not done).  Note
    // that these id's are local keys.
    //
    // Thread-safe: internally, a lock is used.
    int append_node(int64_t node_id, const std::vector<std::string>& attrs);

    // Thread-safe: internally, a lock is used.
    int append_edge(
        int64_t src,
        int64_t dst,
        int64_t atype,
        int64_t timestamp,
        const std::string& attr);

    // An incomplete and/or modified set of Succinct Graph API below

    void get_attribute(std::string& result, int64_t node_id, int attr);

    void get_nodes(
        std::set<int64_t>& result,
        int attr,
        const std::string& search_key);

    void get_nodes(
        std::set<int64_t>& result,
        int attr1,
        const std::string& search_key1,
        int attr2,
        const std::string& search_key2);

    inline std::vector<SuccinctGraph::Assoc> assoc_range(
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len)
    {
        return edge_table_.assoc_range(src, atype, off, len);
    }

    void obj_get(std::vector<std::string>& result, int64_t obj_id);

    inline std::vector<SuccinctGraph::Assoc> assoc_get(
        int64_t src,
        int64_t atype,
        const std::set<int64_t>& dst_id_set,
        int64_t t_low,
        int64_t t_high)
    {
        return edge_table_.assoc_get(src, atype, dst_id_set, t_low, t_high);
    }

    inline int64_t assoc_count(int64_t src, int64_t atype) {
        return edge_table_.assoc_count(src, atype);
    }

    inline std::vector<SuccinctGraph::Assoc> assoc_time_range(
        int64_t src,
        int64_t atype,
        int64_t t_low,
        int64_t t_high,
        int32_t len)
    {
        return edge_table_.assoc_time_range(src, atype, t_low, t_high, len);
    }

    inline void build_backfill_edge_updates(
        std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
        int num_shards_to_mod)
    {
        edge_table_.build_backfill_edge_updates(
            edge_updates, num_shards_to_mod);
    }

    inline int64_t num_nodes() {
        return node_table_->get_num_keys();
    }

private:
    const std::string node_file_, edge_file_;
    std::string node_pointer_file;

    std::shared_ptr<KVLogStore> node_table_ = nullptr;
    FileLogStoreEdgeTable edge_table_;
//    StructuredEdgeTable edge_table_;

    const int max_num_edges_; // bound per log store

};

#endif
