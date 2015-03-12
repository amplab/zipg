#include <iostream>
#include <fstream>
#include <unistd.h>

#include "../../include/succinct-graph/SuccinctGraph.hpp"
#include "../include/SuccinctGraphBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-c creates query files] [-w warmup_query_file] [-q query_file] [-b file_size_type (KB, MB, GB)] [-o output_file] [file]\n", exec);
}

std::string get_file_size(std::string filename, std::string format) {
    struct stat stat_buf;
    int rc = stat(filename.c_str(), &stat_buf);
    if (rc == 0) {
        int shift;
        if (format == "MB") {
            shift = 20;
        } else if (format == "GB") {
            shift = 30;
        } else { // treat default as KB
            shift = 10;
            format = "KB";
        }
        return std::to_string(stat_buf.st_size >> shift) + " " + format;
    } else {
        return "file size could not be determined.";
    }
}

int main(int argc, char **argv) {
    if(argc < 2 || argc > 14) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    std::string size = "MB";
    bool create_file = false;
    std::string warmup_query_file = "warmup.txt";
    std::string measure_query_file = "query_file.txt";
    std::string result_file_name = "benchmark_results.txt";
    while((c = getopt(argc, argv, "t:cw:q:b:o:")) != -1) {
        switch(c) {
        case 't':
            type = std::string(optarg);
            break;
        case 'c':
            create_file = true;
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

    std::string inputpath = std::string(argv[optind]);
    std::ifstream input(inputpath);
    std::ofstream result_file(result_file_name, std::ios_base::app);

    SuccinctGraph * graph = new SuccinctGraph(inputpath);
    // Serialize and save to file
    std::ofstream s_out(inputpath + ".succinct");
    graph->serialize(s_out);
    std::string original_size = get_file_size(inputpath, size);
    std::string succinct_size = get_file_size(inputpath + ".succinct", size);
    s_out.close();

    SuccinctGraphBenchmark s_bench(graph, create_file, warmup_query_file, measure_query_file);
    if(type == "neighbor-throughput") {
        std::pair<double, double> thput_pair = s_bench.benchmark_neighbor_throughput();
        result_file << inputpath << "\n";
        result_file << "Nodes: " << graph->num_nodes() << "\n";
        result_file << "Edges: " << graph->num_edges() << "\n";
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
        result_file << "Original file size: " << original_size << "\n";
        result_file << "Succinct file size: " << succinct_size << "\n\n";
    } else {
        assert(0); // Not supported
    }

    return 0;
}
