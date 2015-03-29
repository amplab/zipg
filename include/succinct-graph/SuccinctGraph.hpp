#ifndef SUCCINCT_GRAPH_H
#define SUCCINCT_GRAPH_H

#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <list>

#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <string>
#include <fstream>
#include <streambuf>

#include "../succinct/SuccinctShard.hpp"

class SuccinctGraph {
private:
    SuccinctShard * shard;
    std::string graph_file;

    int64_t nodes, edges;

public:
    static const int NAME_SIZE;

    SuccinctGraph(std::string node_file, std::string edge_file);
    SuccinctGraph(std::string graph_file);

    int64_t num_nodes();
    int64_t num_edges();

    void get_name(std::string& result, int64_t key);
    void get_nodes(std::set<int64_t>& result, std::string name);
    void get_neighbors(std::string& result, int64_t key);

    size_t serialize(std::ostream& out);

private:
    static const std::string NAME_DELIMINATOR;

    std::string format_input_data(std::string node_file, std::string edge_file);
    size_t lines_in_file(std::string file_path);
};

#endif
