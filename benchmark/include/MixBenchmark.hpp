#ifndef SUCCINCT_GRAPH_MIX_BENCHMARK_H
#define SUCCINCT_GRAPH_MIX_BENCHMARK_H

#include <random>
#include "../../external/succinct-cpp/benchmark/include/Benchmark.hpp"
#include "../../include/succinct-graph/SuccinctGraph.hpp"

class MixBenchmark : public Benchmark {

private:
    int WARMUP_N = 20000;
    int MEASURE_N = 100000;
    int COOLDOWN_N = 1000;

    void read_node_queries(std::string warmup_node_file, std::string query_node_file) {
        std::ifstream warmup_input(warmup_node_file);
        std::ifstream query_input(query_node_file);

        std::string line;
        while (getline(warmup_input, line)) {
            int split_pos = line.find(',');
            int attr = std::atoi(line.substr(0, split_pos).c_str());
            std::string search = line.substr(split_pos + 1);
            warmup_attr.push_back(attr);
            warmup_queries.push_back(search);
        }
        while (getline(query_input, line)) {
            int split_pos = line.find(',');
            int attr = std::atoi(line.substr(0, split_pos).c_str());
            std::string search = line.substr(split_pos + 1);
            queries_attr.push_back(attr);
            queries.push_back(search);
        }
        warmup_input.close();
        query_input.close();
    }

    void read_neighbor_queries(std::string warmup_neighbor_file, std::string query_neighbor_file) {
        std::ifstream warmup_input(warmup_neighbor_file);
        std::ifstream query_input(query_neighbor_file);

        std::string line;
        while (getline(warmup_input, line)) {
            warmup_neighbor_indices.push_back(std::strtoll(line.c_str(), NULL, 10));
        }

        while (getline(query_input, line)) {
            neighbor_indices.push_back(std::strtoll(line.c_str(), NULL, 10));
        }
        warmup_input.close();
        query_input.close();
    }

public:

    MixBenchmark(SuccinctGraph *graph, int warmup, int measure, int cooldown,
            std::string warmup_node_file, std::string query_node_file,
            std::string warmup_neighbor_file, std::string query_neighbor_file) : Benchmark() {
        this->graph = graph;
        WARMUP_N = warmup; MEASURE_N = measure; COOLDOWN_N = cooldown;
        read_node_queries(warmup_node_file, query_node_file);
        read_neighbor_queries(warmup_neighbor_file, query_neighbor_file);
    }

    void benchmark_mix_latency(std::string res_path) {
        std::ofstream res_stream(res_path);
        try {
            // Warmup phase
            std::cout << "Warming up for " << WARMUP_N << " trials\n";
            int warmup_size = warmup_queries.size();
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    std::string result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_size]);
                    if (result.length() == 0) {
                        printf("Error getting neighbors for %d\n", warmup_neighbor_indices[i % warmup_size]);
                    }
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, warmup_attr[i % warmup_size], warmup_queries[i % warmup_size]);
                    if (result.size() == 0) {
                        printf("Error searching for attr %d for %s\n", warmup_attr[i % warmup_size], warmup_queries[i % warmup_size].c_str());
                    }
                }
            }

            // Measure phase
            std::cout << "Measuring latency, " << MEASURE_N << " trials\n";
            int size = queries.size();
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 2 == 0) {
                    std::string result;
                    time_t query_start = get_timestamp();
                    graph->get_neighbors(result, neighbor_indices[i % size]);
                    time_t query_end = get_timestamp();
                    if (result.size() == 0) {
                        printf("Error getting neighbors for %d\n", neighbor_indices[i % size]);
                    } else {
                        res_stream << result.size() << "," <<  (query_end - query_start) << "\n";
                    }
                } else {
                    std::set<int64_t> result;
                    time_t query_start = get_timestamp();
                    graph->search_nodes(result, queries_attr[i % size], queries[i % size]);
                    time_t query_end = get_timestamp();
                    if (result.size() == 0) {
                        printf("Error searching for attr %d for %s\n", queries_attr[i % size], queries[i % size].c_str());
                    } else {
                        res_stream << result.size() << "," <<  (query_end - query_start) << "\n";
                    }
                }
            }

            // Cooldown phase
            for (int i = 0; i < COOLDOWN_N; i++) {
                if (i % 2 == 0) {
                    std::string result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_size]);
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, warmup_attr[i % warmup_size], warmup_queries[i % warmup_size]);
                }
            }

        } catch (std::exception &e) {
            fprintf(stderr, "Throughput test ends...\n");
        }
    }

    double benchmark() {
        double thput = 0;

        try {
            // Warmup phase
            std::cout << "Warming up" << std::endl;
            int warmup_size = warmup_queries.size();
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    std::string result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_size]);
                    if (result.length() == 0) {
                        printf("Error getting neighbors for %d\n", warmup_neighbor_indices[i % warmup_size]);
                        std::exit(1);
                    }
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, warmup_attr[i % warmup_size], warmup_queries[i % warmup_size]);
                    if (result.size() == 0) {
                        printf("Error searching for attr %d for %s\n", warmup_attr[i % warmup_size], warmup_queries[i % warmup_size].c_str());
                        std::exit(1);
                    }
                }
            }

            // Measure phase
            double totsecs = 0;
            std::cout << "Measuring throughput" << std::endl;
            int size = queries.size();
            for (int i = 0; i < MEASURE_N; i++) {
                time_t query_start = get_timestamp();
                if (i % 2 == 0) {
                    std::string result;
                    graph->get_neighbors(result, neighbor_indices[i % size]);
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, queries_attr[i % size], queries[i % size]);
                }
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (double(1E6));
            }
            thput = ((double) MEASURE_N / totsecs);
            printf("Throughput: %f\n total queries: %d, total time: %f\n\n", thput, MEASURE_N, totsecs);

            // Cooldown phase
            for (int i = 0; i < COOLDOWN_N; i++) {
                if (i % 2 == 0) {
                    std::string result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_size]);
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, warmup_attr[i % warmup_size], warmup_queries[i % warmup_size]);
                }
            }

        } catch (std::exception &e) {
            fprintf(stderr, "Throughput test ends...\n");
        }

        return thput;
    }

private:
    std::vector<int> warmup_attr;
    std::vector<std::string> warmup_queries;
    std::vector<int> queries_attr;
    std::vector<std::string> queries;
    std::vector<int> warmup_neighbor_indices;
    std::vector<int> neighbor_indices;
    SuccinctGraph *graph;
};

#endif
