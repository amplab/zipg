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

    size_t nodes, edges;
    std::vector<int64_t> value_offsets;

    uint32_t id;

public:
    static const int64_t MAX_NODES = 1L << 32;

    SuccinctGraph(std::string datafile);

    size_t num_nodes();

    void get_neighbors(std::string& result, int64_t key);

    void access(std::string& result, int64_t key, int32_t len);

    int64_t count(std::string str);

    void search(std::set<int64_t>& result, std::string str);

private:
    std::string format_data_file(std::string datafile);
};

#endif
