#include <iostream>
#include <fstream>
#include <unistd.h>

#include "succinct-graph/SuccinctGraph.hpp"
#include "../include/GraphBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-x warmup_n] [-y measure_n] [-w warmup_file] [-q query_file] [-a neighbor_warmup ] [-b neighbor_query] [-o output_file] [succinct_dir]\n", exec);
}

int main(int argc, char **argv) {
    if(argc < 2) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    int warmup_n = 20000; int measure_n = 100000;
    std::string warmup_query_file = "warmup.txt";
    std::string measure_query_file = "query_file.txt";
    std::string warmup_neighbor_file = "warmup.txt";
    std::string measure_neighbor_file = "query_file.txt";
    std::string result_file_name = "benchmark_results.txt";
    while((c = getopt(argc, argv, "t:x:y:z:w:q:a:b:o:")) != -1) {
        switch(c) {
        case 't':
            type = std::string(optarg);
            break;
        case 'x':
            warmup_n = std::stoi(std::string(optarg));
            break;
        case 'y':
            measure_n = std::stoi(std::string(optarg));
            break;
        case 'w':
            warmup_query_file = std::string(optarg);
            break;
        case 'q':
            measure_query_file = std::string(optarg);
            break;
        case 'a':
            warmup_neighbor_file = std::string(optarg);
            break;
        case 'b':
            measure_neighbor_file = std::string(optarg);
            break;
        case 'o':
            result_file_name = std::string(optarg);
            break;
        }
    }

    if(optind == argc) {
        print_usage(argv[0]);
        return -1;
    }

    std::string succinct_dir = std::string(argv[optind]);
    SuccinctGraph * graph = new SuccinctGraph(succinct_dir, false);

    std::ofstream result_file(result_file_name, std::ios_base::app);

    GraphBenchmark bench(graph);
    if (type == "neighbor-latency") {
        bench.benchmark_neighbor_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "neighbor-throughput") {
        std::pair<double, double> thput_pair = bench.benchmark_neighbor_throughput(
                warmup_query_file, measure_query_file);
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
    } else if (type == "node-latency") {
        bench.benchmark_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "node-throughput") {
        double thput = bench.benchmark_node_throughput(warmup_query_file, measure_query_file);
        result_file << "Get Name Throughput: " << thput << "\n\n";
    } else if (type == "mix-throughput") {
        double thput = bench.benchmark_mix_throughput(
                warmup_neighbor_file, measure_neighbor_file,
                warmup_query_file, measure_query_file);
        result_file << "Mix throughput: " << thput << "\n\n";
    } else if (type == "mix-latency") {
        bench.benchmark_mix_latency(result_file_name, warmup_n, measure_n,
                warmup_neighbor_file, measure_neighbor_file,
                warmup_query_file, measure_query_file);
    } else if (type == "node-node-latency") {
        bench.benchmark_node_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "neighbor-node-latency") {
        bench.benchmark_neighbor_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else {
        assert(0); // Not supported
    }

    return 0;
}
