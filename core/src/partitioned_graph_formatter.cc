#include "partitioned_graph_formatter.h"

#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"
#include "partitioners.hpp"

void PartitionedGraphFormatter::coalescing_gen_assoc_shards(
    const std::vector<std::string>& input_parts,
    char edge_inner_delim,
    char edge_end_delim,
    const int num_atype,
    const int num_shards,
    const int bytes_per_attr,
    const std::string& attr_file,
    const std::string& output_file_prefix)
{
    // Prepare output .assoc ofstreams
    int num_shards_digits = GraphPartitioner::num_digits(num_shards);
    std::vector<std::shared_ptr<std::ofstream>> shard_edge_outs;
    for (int i = 0; i < num_shards; ++i) {
        std::string out_name(GraphPartitioner::format_out_name(
            output_file_prefix, num_shards_digits, i, num_shards));
        shard_edge_outs.push_back(
            std::make_shared<std::ofstream>(out_name));
    }

    // Stream through each input part, for each edge, generate data & output
    std::string line, str;
    int64_t src, dst;
    SuccinctGraph::Assoc assoc;
    std::ifstream attr_in_stream(attr_file);

    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> atype_dis(0, num_atype - 1);
    std::uniform_int_distribution<int> time_dis(
        0, std::numeric_limits<int>::max());

    for (auto& input_name : input_parts) {
        std::ifstream edge_list_part(input_name);
        while (std::getline(edge_list_part, line)) {
            std::stringstream ss(line);
            std::getline(ss, str, edge_inner_delim);
            src = std::stoll(str);
            std::getline(ss, str, edge_end_delim);
            dst = std::stoll(str);

            GraphFormatter::make_rand_assoc(
                assoc,
                src, dst,
                attr_file, attr_in_stream, bytes_per_attr,
                atype_dis, rng1, time_dis, rng2);

            *(shard_edge_outs[src % num_shards])
                << src << " "
                << dst << " "
                << assoc.atype << " "
                << assoc.time << " "
                << assoc.attr << std::endl;
        }
    }
}
