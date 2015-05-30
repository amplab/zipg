#ifndef SUCCINCT_GRAPH_H
#define SUCCINCT_GRAPH_H

#include "../succinct/SuccinctShard.hpp"

class SuccinctGraph {
private:
    SuccinctShard * shard;
    std::string succinct_dir;

    int64_t nodes, edges;

public:
    SuccinctGraph(std::string file, bool construct);

    std::string succinct_directory();
    int64_t num_nodes();
    int64_t num_edges();
    int64_t num_attributes();
    const static std::string DELIMINATORS;

    void get_neighbors(std::set<int64_t>& result, int64_t key);
    void get_attribute(std::string& result, int64_t node_id, int attr);
    void search_nodes(std::set<int64_t>& result, int attr, std::string search_key);
    void search_nodes(std::set<int64_t>& result, int attr1, std::string search_key1,
                                                 int attr2, std::string search_key2);
    void get_neighbors_of_node(std::set<int64_t>& result, int64_t node_id,
                                    int attr, std::string search_key);

    size_t storage_size();
    size_t serialize();

};

#endif
