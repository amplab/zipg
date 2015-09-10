#include "partitioned_graph_formatter.h"

#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"
#include "partitioners.hpp"
#include "utils.h"
#include "utils/thread_pool.h"

using boost::shared_ptr;

void PartitionedGraphFormatter::coalescing_gen_assoc_shards(
    const std::vector<std::string>& input_parts,
    char edge_inner_delim,
    char edge_end_delim,
    int num_atype,
    int num_shards,
    int bytes_per_attr,
    std::string& attr_file,
    std::string& output_file_prefix)
{
    // Prepare output .assoc ofstreams
    int num_shards_digits = GraphPartitioner::num_digits(num_shards);
    std::vector<shared_ptr<std::ofstream>> shard_edge_outs;

    for (int i = 0; i < num_shards; ++i) {
        std::string out_name(GraphPartitioner::format_out_name(
            output_file_prefix, num_shards_digits, i, num_shards));
        shard_edge_outs.push_back(
            shared_ptr<std::ofstream>(new std::ofstream(out_name)));
    }

    std::vector<shared_ptr<std::mutex>> mutexes_for_out_shards;
    for (int i = 0; i < input_parts.size(); ++i) {
        mutexes_for_out_shards.emplace_back(
            shared_ptr<std::mutex>(new std::mutex()));
    }

    ThreadPool pool(200);

    std::vector<shared_ptr<std::thread>> threads;
    for (auto input_part : input_parts) {
        pool.Enqueue([=] {
            this->read_partition_gen_shard(
                edge_inner_delim,
                edge_end_delim,
                num_atype,
                num_shards,
                bytes_per_attr,
                attr_file,
                input_part,
                mutexes_for_out_shards,
                shard_edge_outs
            );
            return;
        });

//        threads.push_back(shared_ptr<std::thread>(new std::thread(
//            &PartitionedGraphFormatter::read_partition_gen_shard,
//            this,
//            edge_inner_delim,
//            edge_end_delim,
//            num_atype,
//            num_shards,
//            bytes_per_attr,
//            attr_file,
//            input_part,
//            mutexes_for_out_shards,
//            shard_edge_outs)));
    }
//    for (auto thread_ptr : threads) {
//        if (thread_ptr->joinable()) {
//            thread_ptr->join();
//        }
//    }
    pool.ShutDown();
}

void PartitionedGraphFormatter::read_partition_gen_shard(
    char edge_inner_delim,
    char edge_end_delim,
    int num_atype,
    int num_shards,
    int bytes_per_attr,
    std::string attr_file,
    std::string partition_file,
    std::vector<shared_ptr<std::mutex>> mutexes_for_out_shards,
    std::vector<shared_ptr<std::ofstream>> shard_edge_outs)
{
    srand(static_cast<unsigned>(time(0)));
    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> atype_dis(0, num_atype - 1);
    std::uniform_int_distribution<int> time_dis(
        0, std::numeric_limits<int>::max());

    // Stream through each input part, for each edge, generate data & output
    std::string line, str;
    int64_t src, dst;
    SuccinctGraph::Assoc assoc;
    std::ifstream attr_in_stream(attr_file);

    const int output_buf_size = 5000;
    std::map<int, std::vector<SuccinctGraph::Assoc>> shard_bufs;

    std::ifstream edge_list_part(partition_file);
    int shard_id;
    while (std::getline(edge_list_part, line)) {
        std::stringstream ss(line);
        std::getline(ss, str, edge_inner_delim);
        src = std::stoll(str);
        std::getline(ss, str, edge_end_delim);
        dst = std::stoll(str);
        shard_id = src % num_shards;

        // No race on attr file, because have my own attr_in_stream
        // there may be compressibility issue since there's a risk that these
        // threads generate same/similar sequence of attributes
        GraphFormatter::make_rand_assoc(
            assoc,
            src, dst,
            attr_file, attr_in_stream, bytes_per_attr,
            atype_dis, rng1, time_dis, rng2);

        auto& buf = shard_bufs[shard_id];
        buf.push_back(assoc);

        if (buf.size() == output_buf_size) {
            std::lock_guard<std::mutex> lk(*(mutexes_for_out_shards[shard_id]));

            for (auto& assoc : buf) {
                *(shard_edge_outs[shard_id])
                    << assoc.src_id << " "
                    << assoc.dst_id << " "
                    << assoc.atype << " "
                    << assoc.time << " "
                    << assoc.attr << std::endl;
            }

            buf.clear();
        }
    }

    for (auto& entry : shard_bufs) {
        shard_id = entry.first;
        auto buf = entry.second;
        std::lock_guard<std::mutex> lk(*(mutexes_for_out_shards[shard_id]));

        for (auto& assoc : buf) {
            *(shard_edge_outs[shard_id])
                << assoc.src_id << " "
                << assoc.dst_id << " "
                << assoc.atype << " "
                << assoc.time << " "
                << assoc.attr << std::endl;
        }

        buf.clear();
    }
}

int main(int argc, char **argv) {
    std::string output_file_prefix(argv[1]);
    int num_shards = std::stoi(argv[2]);
    std::string attr_file(argv[3]); // TPC-H
    int edge_attr_size = std::stoi(argv[4]);
    char edge_inner_delim = std::string(argv[5]).at(0);
    char edge_end_delim = std::string(argv[6]).at(0);

    std::vector<std::string> input_parts;
    for (int i = 8; i < argc; ++i) {
        input_parts.emplace_back(argv[i]);
    }

    LOG_E("Calling coalescing gen on %d input partitions, for %d shards\n",
        input_parts.size(), num_shards);

    PartitionedGraphFormatter pgf;
    pgf.coalescing_gen_assoc_shards(
        input_parts,
        edge_inner_delim,
        edge_end_delim,
        5,
        num_shards,
        edge_attr_size,
        attr_file,
        output_file_prefix);

}
