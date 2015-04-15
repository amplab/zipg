#include <iostream>
#include <fstream>
#include <unistd.h>

#include "succinct-graph/SuccinctGraph.hpp"
#include "../include/NeighborBenchmark.hpp"
#include "../include/NodeBenchmark.hpp"
#include "../include/MixBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-x warmup_n] [-y measure_n] [-z cooldown_n] [-w warmup_file] [-q query_file] [-a neighbor_warmup ] [-b neighbor_query] [-o output_file] [graph_file]\n", exec);
}

int main(int argc, char **argv) {
    if(argc < 2) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    int warmup_n = 20000; int measure_n = 100000; int cooldown_n = 1000;
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
        case 'z':
            cooldown_n = std::stoi(std::string(optarg));
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

    std::string graph_file = std::string(argv[optind]);
    SuccinctGraph * graph = new SuccinctGraph(graph_file);

    std::ofstream result_file(result_file_name, std::ios_base::app);
    result_file << graph_file << "\n";

    std::cout << "Benching " << graph_file << std::endl;
    if (type == "neighbor-latency") {
        NeighborBenchmark bench(graph, warmup_query_file, measure_query_file);
        bench.benchmark_neighbor_latency(result_file_name, warmup_n, measure_n, cooldown_n);
    } else if (type == "neighbor-throughput") {
        NeighborBenchmark s_bench(graph, warmup_query_file, measure_query_file);
        std::pair<double, double> thput_pair = s_bench.benchmark_neighbor_throughput();
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
    } else if (type == "node-latency") {
        NodeBenchmark bench(graph, warmup_query_file, measure_query_file);
        bench.benchmark_name_latency(result_file_name, warmup_n, measure_n, cooldown_n);
    } else if (type == "name-throughput") {
        NodeBenchmark s_bench(graph, warmup_query_file, measure_query_file);
        double thput = s_bench.benchmark_name_throughput();
        result_file << "Get Name Throughput: " << thput << "\n\n";
    } else if (type == "mix-throughput") {
        MixBenchmark m_bench(graph, warmup_n, measure_n, cooldown_n,
                warmup_query_file, measure_query_file,
                warmup_neighbor_file, measure_neighbor_file);
        double thput = m_bench.benchmark_name_throughput();
        result_file << "Mix throughput: " << thput << "\n\n";
    } else {
        assert(0); // Not supported
    }

    return 0;
}
