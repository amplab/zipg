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
    std::string input_datafile;

    int64_t nodes, edges;

public:
    static const int64_t MAX_NODES = 1L << 32;

    SuccinctGraph(std::string datafile);

    int64_t num_nodes();
    int64_t num_edges();

    void get_neighbors(std::string& result, int64_t key);

    size_t serialize(std::ostream& out);

private:
    std::string format_data_file(std::string datafile);
};

#endif
