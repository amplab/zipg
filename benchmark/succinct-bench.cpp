#include <iostream>
#include <fstream>
#include <unistd.h>

#include "../include/succinct-graph/SuccinctGraph.hpp"
#include "../include/succinct-graph/bench/SuccinctGraphBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-m mode] [-t type] [file]\n", exec);
}

int main(int argc, char **argv) {
    if(argc < 2 || argc > 5) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    uint32_t mode = 0;
    std::string type = "neighbor-throughput";
    int32_t len = 100;
    while((c = getopt(argc, argv, "m:t:")) != -1) {
        switch(c) {
        case 'm':
            mode = atoi(optarg);
            break;
        case 't':
            type = std::string(optarg);
            break;
        default:
            mode = 0;
            type = "neighbor-throughput";
        }
    }

    if(optind == argc) {
        print_usage(argv[0]);
        return -1;
    }

    std::string inputpath = std::string(argv[optind]);
    std::ifstream input(inputpath);

    SuccinctGraph *graph;
    if(mode == 0) {
        graph = new SuccinctGraph(inputpath);

        // Serialize and save to file
        // std::ofstream s_out(inputpath + ".succinct");
        // fd->serialize(s_out);
        // s_out.close();
    } else {
        // Only modes 0 supported for now
        assert(0);
    }

    SuccinctBenchmark s_bench(graph);
    if(type == "latency-get") {
        //s_bench.benchmark_get_latency("latency_results_get");
    } else if(type == "neighbor-throughput") {
        s_bench.benchmark_neighbor_throughput();
    } else {
        // Not supported
        assert(0);
    }

    return 0;
}
