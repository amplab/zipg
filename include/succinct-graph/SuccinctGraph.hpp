#ifndef SUCCINCT_GRAPH_H
#define SUCCINCT_GRAPH_H

#include "../succinct/SuccinctShard.hpp"
#include "../succinct/SuccinctFile.hpp"

class SuccinctGraph {
public:
    // Constructor.  This doesn't actually build the internal data structures.
    SuccinctGraph(std::string succinct_dir = 0,
                  bool construct = false,
                  uint32_t sa_sampling_rate = 32,
                  uint32_t isa_sampling_rate = 32,
                  uint32_t npa_sampling_rate = 128);

    // Loads the previously constructed node table & edge table.
    SuccinctGraph(std::string node_succinct_dir, std::string edge_succinct_dir);

    /** Setters that can modify default settings. */
    SuccinctGraph& set_npa_sampling_rate(uint32_t sampling_rate);
    SuccinctGraph& set_sa_sampling_rate(uint32_t sampling_rate);
    SuccinctGraph& set_isa_sampling_rate(uint32_t sampling_rate);

    // Constructs the node/edge tables and Succinct-encodes them, using
    // previously specified (possibly default) settings.
    //
    //   node_file: each row contains attributes (bytes) for the node
    //              whose ID is current row number - 1, separated by
    //              unique delimiters
    //   edge_file: each row represents one association, in format
    //              srcId dstId atype time [everything from here to EOL is attr]
    // FIXME: probably makes sense to add & to params
    SuccinctGraph& construct(std::string node_file, std::string edge_file);

    std::string succinct_directory();

    int64_t num_nodes();
    int64_t num_edges();
    int64_t num_attributes();

    size_t storage_size();
    size_t serialize();

    /**************** Internal formats ****************/
    // C.f. the LinkBench paper, Sigmoid 2013
    typedef int64_t NodeId;
    typedef int64_t Timestamp;
    typedef int64_t AType;

    typedef std::pair<NodeId, AType> AssocListKey;

    struct Assoc {
        NodeId src_id; // 8 bytes
        NodeId dst_id; // 8 bytes
        AType atype; // 8 bytes
        Timestamp time; // 8 bytes
        std::string attr; // variable bytes
    };

    static bool cmp_assoc_by_decreasing_time(const Assoc &a, const Assoc &b) {
        return a.time > b.time;
    }

    /**************** Primitive APIs ****************/

    void get_attribute(std::string& result, int64_t node_id, int attr);

    void get_neighbors(std::vector<int64_t>& result, int64_t node);

    void get_neighbors(
        std::vector<int64_t>& result,
        int64_t node,
        int attr,
        std::string search_key);

    void get_nodes(std::set<int64_t>& result, int attr, std::string search_key);

    void get_nodes(
        std::set<int64_t>& result,
        int attr1,
        std::string search_key1,
        int attr2,
        std::string search_key2);

    /**************** TAO-like APIs ****************/

    static void
    print_assoc_results(const std::vector<Assoc>& assoc_results) {
        for (auto it = assoc_results.begin(); it != assoc_results.end(); ++it) {
            printf("[ %lld-->%lld, atype %lld, time %lld, attr '%s' ]\n",
                it->src_id, it->dst_id, it->atype, it->time, it->attr.c_str());
        }
        printf("\n");
    }

    // Gets the attribute data of node `obj_id` into `result`.
    void obj_get(std::string& result, int64_t obj_id);

    // All arguments can be optional (use -1 for none) with the natural
    // semantics.
    std::vector<Assoc> assoc_range(
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len);

    // All arguments, except for `dst_id_set`, can be optional (use -1 for
    // none) with the natural semantics.
    std::vector<Assoc> assoc_get(
        int64_t src,
        int64_t atype,
        std::set<int64_t> dst_id_set,
        int64_t t_low,
        int64_t t_high);

    // Returns number of associations in the association list (src, atype).
    // Undefined behavior if (src, atype) doesn't exist.
    // All arguments can be optional.
    int64_t assoc_count(int64_t src, int64_t atype);

    // All arguments can be optional.
    std::vector<Assoc> assoc_time_range(
        int64_t src,
        int64_t atype,
        int64_t t_low,
        int64_t t_high,
        int32_t len);

    /**************** Fields ****************/

    // Succinct compression params: currently same for node table & edge table.
    uint32_t sa_sampling_rate = 64;
    uint32_t isa_sampling_rate = 64;
    uint32_t npa_sampling_rate = 256;

    const static std::string DELIMINATORS;

private:
    SuccinctShard *node_table;
    SuccinctFile *edge_table;

    std::string succinct_dir;
    int64_t edges;

    // Returns a list of edge table offsets; result is a list since the two
    // arguments can be omitted (i.e. as wildcards, represented as -1 for now).
    // An edge table offset is -1 iff an assoc list doesn't exist.
    std::vector<int64_t> get_edge_table_offsets(NodeId id, AType atype);

};

#endif
