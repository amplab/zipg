#ifndef GRAPH_SUFFIX_STORE_H
#define GRAPH_SUFFIX_STORE_H

#include "FileSuffixStore.h"
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
    GraphSuffixStore(
        const std::string& node_file,
        const std::string& edge_file)
        : node_file_(node_file),
          edge_file_(edge_file)
    { }

    void init(int option = 1);

    // Bulk load, assuming init() has not been called.  The new files override
    // the ones passed in by the constructor.
    void init(
        const std::string& node_file,
        const std::string& edge_file,
        int option = 1);

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

private:

    std::string node_file_, edge_file_;

    std::shared_ptr<KVSuffixStore> node_table_ = nullptr;
    std::shared_ptr<FileSuffixStore> edge_table_ = nullptr;

};

#endif
