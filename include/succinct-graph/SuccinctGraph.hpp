#ifndef SUCCINCT_GRAPH_H
#define SUCCINCT_GRAPH_H

#include "../succinct/SuccinctShard.hpp"

class SuccinctGraph {
private:
    SuccinctShard * shard;
    std::string succinct_dir;

    int64_t nodes, edges;

public:
    SuccinctGraph(std::string file, bool construct,
        uint32_t sa_sampling_rate = 32,
        uint32_t isa_sampling_rate = 32,
        uint32_t npa_sampling_rate = 128);

    std::string succinct_directory();
    int64_t num_nodes();
    int64_t num_edges();
    int64_t num_attributes();
    const static std::string DELIMINATORS;

    void get_attribute(std::string& result, int64_t node_id, int attr);

    void get_neighbors(std::vector<int64_t>& result, int64_t key);
    void get_neighbors_of_node(std::vector<int64_t>& result, int64_t node_id,
        int attr, std::string search_key);

    void search_nodes(std::set<int64_t>& result, int attr, std::string search_key);
    void search_nodes(std::set<int64_t>& result, int attr1, std::string search_key1,
                                                 int attr2, std::string search_key2);

    size_t storage_size();
    size_t serialize();

};

#endif
