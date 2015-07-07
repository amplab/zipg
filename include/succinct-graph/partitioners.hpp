#ifndef GRAPH_PARTITIONER_H
#define GRAPH_PARTITIONER_H

#include <cassert>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

class GraphPartitioner {
public:
    virtual ~GraphPartitioner() {};

    virtual void partition(
        const std::string& node_file_in,
        const std::string& edge_file_in) = 0;
};

class RangePartitioner : public GraphPartitioner {
public:
    RangePartitioner(int32_t num_shards) : num_shards_(num_shards) {};
    ~RangePartitioner() {};

    // Assumes consecutive & in-order: line i of node_file_in must contain
    // attributes for node i-1.  Similarly, edge_file_in should contains edges
    // sorted by srcId.
    //
    // Each input file is partitioned into <= `num_shards_` splits.  Specifics:
    // - # Node file splits < numShards iff number of lines in
    //   node file < numShards.
    // - # edge file splits can contain "gaps".
    //
    // However, this never outputs empty files; so empty shards are represented
    // by the absence of a split.
    void partition(
        const std::string& node_file_in,
        const std::string& edge_file_in);

    int32_t num_shards_ = 1;
};

#endif
