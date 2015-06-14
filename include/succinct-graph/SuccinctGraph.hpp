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

    /** Setters that can modify default settings. */
    SuccinctGraph& set_npa_sampling_rate(uint32_t sampling_rate);
    SuccinctGraph& set_sa_sampling_rate(uint32_t sampling_rate);
    SuccinctGraph& set_isa_sampling_rate(uint32_t sampling_rate);

    // Constructs the node/edge tables and Succinct-encodes them, using
    // previously specified (possibly default) settings.
    //
    //   node_file: each row contains attributes (bytes) for the node
    //              whose ID is current row number - 1
    //   edge_file: each row represents one association, in format
    //              srcId dstId atype time [everything from here to \n is attr]
    // FIXME: probably makes sense to add & to params
    SuccinctGraph& construct(std::string node_file, std::string edge_file);

    // Loads the previously constructed node table & edge table.
    SuccinctGraph& load(std::string node_succinct_dir,
                        std::string edge_succinct_dir);

    std::string succinct_directory();

    int64_t num_nodes();
    int64_t num_edges();
    int64_t num_attributes();

    size_t storage_size();
    size_t serialize();

    /**************** Old APIs ****************/
    // TODO: clean up GraphBenchmark so that we can remove these.
    void get_attribute(std::string& result, int64_t node_id, int attr);

    void get_neighbors(std::vector<int64_t>& result, int64_t key);
    void get_neighbors_of_node(std::vector<int64_t>& result, int64_t node_id,
        int attr, std::string search_key);

    void search_nodes(std::set<int64_t>& result, int attr, std::string search_key);
    void search_nodes(std::set<int64_t>& result, int attr1, std::string search_key1,
                                                 int attr2, std::string search_key2);

    /**************** TAO-like APIs ****************/

    // dst_id, timestamp, attr
    typedef std::tuple<uint64_t, uint64_t, std::string> AssocResult;

    static void
    print_assoc_results(const std::vector<AssocResult>& assoc_results) {
        for (auto it = assoc_results.begin(); it != assoc_results.end(); ++it) {
            printf("[id=%lld,time=%lld,attr='%s'] ",
                   std::get<0>(*it),
                   std::get<1>(*it),
                   std::get<2>(*it).c_str());
        }
        printf("\n\n");
    }

    // Gets the attribute data of node `obj_id` into `result`.
    void obj_get(std::string& result, int64_t obj_id);

    std::vector<AssocResult> assoc_range(int64_t src,
                                         int32_t atype,
                                         int32_t off,
                                         int32_t len);

    void assoc_get(
        int64_t src,
        int32_t atype,
        std::set<int64_t> dst_id_set,
        int64_t t_low,
        int64_t t_high);

    // Returns number of associations in the association list (src, atype).
    // Undefined behavior if (src, atype) doesn't exist.
    uint64_t assoc_count(int64_t src, int32_t atype);

    void assoc_time_range(
        int64_t src,
        int32_t atype,
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

    /**************** Internal formats ****************/
    // TODO: rethink types/fields c.f. the LinkBench paper

    typedef uint64_t NodeId;
    typedef uint64_t Timestamp;
    typedef uint32_t AType;
    typedef std::pair<NodeId, AType> AssocListKey;
    struct Assoc {
        NodeId dst_id; // 8 bytes
        Timestamp time; // 8 bytes
        std::string attr; // variable bytes
    };

    static bool cmp_assoc_by_decreasing_time(const Assoc &a, const Assoc &b) {
        return a.time > b.time;
    }

    // Rule: 64 bit mapped to 8 chars, 32 bit mapped to 4 chars.
//    static const int WIDTH_ATYPE = 4; // encoded in str alphabet of size 64
    static const int WIDTH_TIMESTAMP = 8; // encoded in str alphabet of size 64
    static const int WIDTH_NODE_ID = 8; // encoded in str alphabet of size 64
//    static const int WIDTH_EDGEWIDTH = 4;
//    static const int WIDTH_DATAWIDTH = 8;

    uint64_t get_edge_table_offset(NodeId id, AType atype);

};

#endif
