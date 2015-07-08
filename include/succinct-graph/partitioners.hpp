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

    // Assumes consecutive & in-order: line i of node_file_in must contain
    // attributes for node i-1.  Similarly, edge_file_in should contains edges
    // sorted by srcId.
    //
    // Each input file is partitioned into <= `num_shards_` splits. Specifically
    // - Number of node file splits < numShards *iff* number of lines in node
    //   file < numShards.  Otherwise the two are equal.
    // - Edge file splits can contain "gaps" (non-empty splits whose shard IDs
    //   are not consecutive).
    //
    // However, this never outputs empty files; so empty shards are represented
    // by the absence of a split.
    virtual void partition(
        const std::string& node_file_in,
        const std::string& edge_file_in) = 0;

protected:
    static int32_t num_digits(int32_t number) {
       if (number == 0) return 1;
       int32_t digits = 0;
       while (number != 0) {
           number /= 10;
           ++digits;
       }
       return digits;
    };

    static std::string format_out_name(
        const std::string& prefix, int digits, int num)
    {
        char s[digits + 1];
        sprintf(s, "%0*d", digits, num);
        return std::string(prefix + "-part" + s);
    };

};

class RangePartitioner : public GraphPartitioner {
public:
    RangePartitioner(int32_t num_shards) : num_shards_(num_shards) {};
    ~RangePartitioner() {};

    void partition(
        const std::string& node_file_in,
        const std::string& edge_file_in);

    int32_t num_shards_ = 1;
};

class HashPartitioner : public GraphPartitioner {
public:
    HashPartitioner(int32_t num_shards) : num_shards_(num_shards) {};
    ~HashPartitioner() {};

    // Node Id K's records (node attributes, assoc lists) are assigned to
    // shard K % N, where N is the total number of shards in the cluster.
    void partition(
        const std::string& node_file_in,
        const std::string& edge_file_in);

    int32_t num_shards_ = 1;
};

#endif
