#include <iostream>
#include <fstream>
#include <unistd.h>

#include "../../include/succinct-graph/SuccinctGraph.hpp"
#include "../include/NeighborBenchmark.hpp"
#include "../include/NameBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-q query_file] [-b file_size_type (KB, MB, GB)] [-o output_file] [node_file edge_file graph_file]\n", exec);
}

double get_file_size(std::string filename, std::string format) {
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
        return stat_buf.st_size >> shift;
    } else {
        fprintf(stderr, (filename + " file size could not be determined.").c_str());
        return -1;
    }
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
    while((c = getopt(argc, argv, "t:cw:q:b:o:")) != -1) {
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

    std::string node_file = std::string(argv[optind]);
    std::string edge_file = std::string(argv[optind + 1]);
    std::string graph_file = std::string(argv[optind + 2]);
    std::string succinct_file = graph_file + ".succinct";

    std::ifstream graph_input(graph_file);
    SuccinctGraph * graph;
    if (!graph_input.good()) {
        graph = new SuccinctGraph(node_file, edge_file);
        // Serialize and save to file
        std::ofstream s_out(succinct_file);
        graph->serialize(s_out);
        s_out.close();
        printf("Created succinct graph\n");
    } else {
        graph = new SuccinctGraph(graph_file);
    }

    std::ofstream result_file(result_file_name, std::ios_base::app);
    double original_size = get_file_size(node_file, size) + get_file_size(edge_file, size);
    double succinct_size = get_file_size(succinct_file, size);

    result_file << graph_file << "\n";
    result_file << "Nodes: " << graph->num_nodes() << "\n";
    result_file << "Edges: " << graph->num_edges() << "\n";
    result_file << "Original file size: " << original_size << "\n";
    result_file << "Succinct file size: " << succinct_size << "\n\n";

    std::cout << "benching " << graph_file << std::endl;
    if (type == "neighbor-throughput") {
        NeighborBenchmark s_bench(graph, warmup_query_file, measure_query_file);
        std::pair<double, double> thput_pair = s_bench.benchmark_neighbor_throughput();
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
    } else if (type == "name-throughput") {
        NameBenchmark s_bench(graph, warmup_query_file, measure_query_file);
        double thput = s_bench.benchmark_name_throughput();
        result_file << "Get Name Throughput: " << thput << "\n";
    } else {
        assert(0); // Not supported
    }

    return 0;
}
