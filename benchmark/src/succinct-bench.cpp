#include <iostream>
#include <fstream>
#include <unistd.h>

#include "succinct-graph/SuccinctGraph.hpp"
#include "../include/NeighborBenchmark.hpp"
#include "../include/NameBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-q query_file] [-b file_size_type (KB, MB, GB)] [-o output_file] [graph_file]\n", exec);
}

int main(int argc, char **argv) {
    if(argc < 2 || argc > 16) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    std::string size = "MB";
    std::string warmup_query_file = "warmup.txt";
    std::string measure_query_file = "query_file.txt";
    std::string result_file_name = "benchmark_results.txt";
    while((c = getopt(argc, argv, "t:w:q:b:o:")) != -1) {
        switch(c) {
        case 't':
            type = std::string(optarg);
            break;
        case 'w':
            warmup_query_file = std::string(optarg);
            break;
        case 'q':
            measure_query_file = std::string(optarg);
            break;
        case 'b':
            size = std::string(optarg);
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
    std::string succinct_file = graph_file + ".succinct";

    std::ifstream graph_input(graph_file);
    SuccinctGraph * graph = new SuccinctGraph(graph_file);

    std::ofstream result_file(result_file_name, std::ios_base::app);

    result_file << graph_file << "\n";
    result_file << "Nodes: " << graph->num_nodes() << "\n";
    result_file << "Edges: " << graph->num_edges() << "\n";

    std::cout << "Benching " << graph_file << std::endl;
    if (type == "neighbor-throughput") {
        NeighborBenchmark s_bench(graph, warmup_query_file, measure_query_file);
        std::pair<double, double> thput_pair = s_bench.benchmark_neighbor_throughput();
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
    } else if (type == "name-throughput") {
        NameBenchmark s_bench(graph, warmup_query_file, measure_query_file);
        double thput = s_bench.benchmark_name_throughput();
        result_file << "Get Name Throughput: " << thput << "\n\n";
    } else {
        assert(0); // Not supported
    }

    return 0;
}
