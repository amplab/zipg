#ifndef PARTITIONED_GRAPH_FORMATTER_H
#define PARTITIONED_GRAPH_FORMATTER_H

#include "partitioners.hpp"

#include <fstream>
#include <mutex>
#include <random>
#include <thread>

#include <boost/shared_ptr.hpp>

using boost::shared_ptr;

class PartitionedGraphFormatter {
public:

    void read_partition_gen_shard(
        char edge_inner_delim,
        char edge_end_delim,
        int num_atype,
        int num_shards,
        int bytes_per_attr,
        std::string attr_file,
        std::string partition_file,
        std::vector<shared_ptr<std::mutex>> mutexes_for_out_shards ,
        std::vector<shared_ptr<std::ofstream>> shard_edge_outs
    );

    void coalescing_gen_assoc_shards(
        const std::vector<std::string>& input_parts,
        char edge_inner_delim,
        char edge_end_delim,
        int num_atype,
        int num_shards,
        int bytes_per_attr,
        std::string& attr_file,
        std::string& output_file_prefix);

};

#endif
