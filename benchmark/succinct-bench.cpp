#include <iostream>
#include <fstream>
#include <unistd.h>

#include "../include/succinct-graph/SuccinctGraph.hpp"
#include "../include/succinct-graph/bench/SuccinctGraphBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [file]\n", exec);
}

long get_file_size(std::string filename) {
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

int main(int argc, char **argv) {
    if(argc < 2 || argc > 5) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    std::string result_file_name = "benchmark_results";
    while((c = getopt(argc, argv, "t:")) != -1) {
        switch(c) {
        case 't':
            type = std::string(optarg);
            break;
        default:
            type = "neighbor-throughput";
        }
    }

    if(optind == argc) {
        print_usage(argv[0]);
        return -1;
    }

    std::string inputpath = std::string(argv[optind]);
    std::ifstream input(inputpath);
    std::ofstream result_file(result_file_name, std::ios_base::app);

    SuccinctGraph * graph = new SuccinctGraph(inputpath);
    // Serialize and save to file
    std::ofstream s_out(inputpath + ".succinct");
    size_t original_size = get_file_size(inputpath);
    size_t succinct_size = graph->serialize(s_out);
    s_out.close();

    SuccinctGraphBenchmark s_bench(graph);
    if(type == "latency-get") {
        //s_bench.benchmark_get_latency("latency_results_get");
    } else if(type == "neighbor-throughput") {
        std::pair<double, double> thput_pair = s_bench.benchmark_neighbor_throughput();
        result_file << inputpath << "\n";
        result_file << "Nodes: " << graph->num_nodes() << "\n";
        result_file << "Edges: " << graph->num_edges() << "\n";
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
        result_file << "Original file size: " << original_size << "\n";
        result_file << "Succinct file size: " << succinct_size << "\n\n";
    } else {
        // Not supported
        assert(0);
    }

    return 0;
}
