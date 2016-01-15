#ifndef FILE_LOG_STORE_EDGE_TABLE_H
#define FILE_LOG_STORE_EDGE_TABLE_H

#include "FileLogStore.h"
#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"

// This is backed by a flat file.  In the file, the same logical assoc list can
// occur multiple times, and each segment must contain exactly one edge.
// Lastly, we assume that a greater file offset implies a larger timestamp (more
// recent), for the edges within a logical assoc list.
class FileLogStoreEdgeTable {
public:

    // `edge_file` is an unformatted edge list.
    FileLogStoreEdgeTable(const std::string& edge_file)
        : edge_file_(edge_file),
          file_log_store_(edge_file)
    { }

    ~FileLogStoreEdgeTable() {
        if (edge_table_file_ != "") {
            std::remove(edge_table_file_.c_str());
        }
    }

    inline void construct() {
        edge_table_file_ = GraphFormatter::write_to_temp_file("");
        SuccinctGraph::output_edge_table_singleton(
            edge_file_, edge_table_file_);
        COND_LOG_E("Edge table wrote out to '%s'\n", edge_table_file_.c_str());
        file_log_store_.construct(edge_table_file_);
    }

    inline void load() {
        file_log_store_.load();
    }

    // Limitation: we assume timestamp for a particular (src, atype) is
    // monotonically increasing for now (think: social network).
    //
    // NOT thread-safe for concurrent writes: relies on higher-level locking.
    inline void add_assoc(
        int64_t src,
        int64_t dst,
        int64_t atype,
        int64_t timestamp,
        const std::string& attr)
    {
        file_log_store_.append(GraphFormatter::format_assoc_single(
            src, dst, atype, timestamp, attr));
    }

    // Currently, used only by GraphLogStore's edge-append path to guard against
    // adding too many edges into this store.
    //
    // This is a gross hack... The reason to not properly maintain this count is
    // because it's somewhat complicated to loop through the bytes during the
    // initial reading/loading.  Also, locking might be needed.
    int64_t num_edges() {
        return file_log_store_.full()
            ? std::numeric_limits<int64_t>::max()
            : -1;
    }

    // An incomplete and/or modified set of Succinct Graph API below

    std::vector<SuccinctGraph::Assoc> assoc_range(
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len);

    std::vector<SuccinctGraph::Assoc> assoc_get(
        int64_t src,
        int64_t atype,
        const std::set<int64_t>& dst_id_set,
        int64_t t_low,
        int64_t t_high);

    // Makes use of the singleton lists assumption.
    inline int64_t assoc_count(int64_t src, int64_t atype) {
        std::vector<int64_t> eoffs;
        file_log_store_.search(
            eoffs, SuccinctGraph::mk_edge_table_search_key(src, atype));
        return eoffs.size();
    }

    std::vector<SuccinctGraph::Assoc> assoc_time_range(
        int64_t src,
        int64_t atype,
        int64_t t_low,
        int64_t t_high,
        int32_t len);

    inline void build_backfill_edge_updates(
        std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
        int num_shards_to_mod)
    {
        assert(false);
    }

private:

    std::string edge_file_, edge_table_file_;
    FileLogStore file_log_store_;

};

#endif
