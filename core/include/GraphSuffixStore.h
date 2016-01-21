#ifndef GRAPH_SUFFIX_STORE_H
#define GRAPH_SUFFIX_STORE_H

#include "FileSuffixStore.h"
#include "GraphFormatter.hpp"
#include "KVSuffixStore.h"
#include "SuccinctGraph.hpp"

#include <set>
#include <string>
#include <vector>

// A graph that is backed by SuffixStore.  In other words, the Node Table is
// backed by KVSuffixStore, and the Edge Table by FileSuffixStore.
class GraphSuffixStore {
public:

    // The `node_file` here is just the un-delimited node properties (the
    // values file).  The keys are assumed to be 0-based line numbers; the
    // key-value pointers are computed by using newlines as record delimiters.
    //
    // The `edge_file` is the formatted edge table.
    //
    // The `assoc_file_` is the unformatted assoc table -- it is used only for
    // calculating updates, so it is hard-coded.
    GraphSuffixStore(
        const std::string& node_file,
        const std::string& edge_file)
        : node_file_(node_file),
          edge_file_(edge_file),
          assoc_file_(edge_file + "_assoc")
    { }

    void construct();

    void construct(const std::string& node_file, const std::string& edge_file);

    void load();

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

    std::vector<SuccinctGraph::Assoc> assoc_range(
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len);

    void obj_get(std::vector<std::string>& result, int64_t obj_id);

    std::vector<SuccinctGraph::Assoc> assoc_get(
        int64_t src,
        int64_t atype,
        const std::set<int64_t>& dst_id_set,
        int64_t t_low,
        int64_t t_high);

    int64_t assoc_count(int64_t src, int64_t atype);

    std::vector<SuccinctGraph::Assoc> assoc_time_range(
        int64_t src,
        int64_t atype,
        int64_t t_low,
        int64_t t_high,
        int32_t len);

    // Scans through all assocs in this store, and groups them by dstShardId.
    void build_backfill_edge_updates(
        std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
        int num_shards_to_mod);

private:

    std::string node_file_, edge_file_, assoc_file_;

    std::shared_ptr<KVSuffixStore> node_table_ = nullptr;
    std::shared_ptr<FileSuffixStore> edge_table_ = nullptr;

    // Binary search: locates smallest timestamp t, such that t >= t_low.
    // Upon entry, `curr_off` must point to the start of the timestamps of the
    // current association list in the edge table, and `cnt` denotes the number
    // of assocs in this list.  Returns -1 if no such indexes exist.
    int time_range_binary_search_lower_bound(
        int64_t t_low,
        int64_t cnt,
        int64_t curr_off,
        std::string& tmp_token,
        const int32_t timestamp_width);

    // Binary search: locates largest timestamp t, such that t <= t_high.
    int time_range_binary_search_upper_bound(
        int64_t t_high,
        int64_t cnt,
        int64_t curr_off,
        std::string& tmp_token,
        const int32_t timestamp_width);

};

#endif
