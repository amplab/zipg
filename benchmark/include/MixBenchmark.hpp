#ifndef SUCCINCT_GRAPH_MIX_BENCHMARK_H
#define SUCCINCT_GRAPH_MIX_BENCHMARK_H

#include "GraphBenchmark.hpp"
#include "../../include/succinct-graph/SuccinctGraph.hpp"

class MixBenchmark : public GraphBenchmark {
public:

    MixBenchmark(SuccinctGraph *graph, count_t warmup, count_t measure,
            std::string warmup_node_file, std::string query_node_file,
            std::string warmup_neighbor_file, std::string query_neighbor_file) : GraphBenchmark() {
        this->graph = graph;
        WARMUP_N = warmup; MEASURE_N = measure;
        read_node_queries(warmup_node_file, query_node_file);
        read_neighbor_queries(warmup_neighbor_file, query_neighbor_file);
    }

    void benchmark_mix_latency(std::string res_path) {
        std::ofstream res_stream(res_path);
        fprintf(stderr, "Benchmarking mixQuery latency of %s\n", this->graph->succinct_directory().c_str());
        try {
            // Warmup phase
            fprintf(stderr, "Warming up for %lu queries...\n", WARMUP_N);
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    std::set<int64_t> result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i/2]);
                    if (result.size() == 0) {
                        fprintf(stderr, "Error getting neighbors for %d.\n", warmup_neighbor_indices[i/2]);
                    }
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, warmup_attr[i/2], warmup_queries[i/2]);
                    if (result.size() == 0) {
                        fprintf(stderr, "Error searching attr %d for %s\n", warmup_attr[i/2], warmup_queries[i/2].c_str());
                    }
                }
            }
            fprintf(stderr, "Warmup complete.\n");

            // Measure phase
            fprintf(stderr, "Measuring for %lu queries...\n", MEASURE_N);
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 2 == 0) {
                    std::set<int64_t> result;
                    time_t query_start = get_timestamp();
                    graph->get_neighbors(result, neighbor_indices[i/2]);
                    time_t query_end = get_timestamp();
                    if (result.size() == 0) {
                        printf("Error getting neighbors for %d\n", neighbor_indices[i/2]);
                    } else {
                        res_stream << result.size() << "," <<  (query_end - query_start) << "\n";
                    }
                } else {
                    std::set<int64_t> result;
                    time_t query_start = get_timestamp();
                    graph->search_nodes(result, queries_attr[i/2], queries[i/2]);
                    time_t query_end = get_timestamp();
                    if (result.size() == 0) {
                        printf("Error searching for attr %d for %s\n", queries_attr[i/2], queries[i/2].c_str());
                    } else {
                        res_stream << result.size() << "," <<  (query_end - query_start) << "\n";
                    }
                }
            }
            fprintf(stderr, "Measure complete.\n");

            // Cooldown phase
            fprintf(stderr, "Cooling down for %lu queries...\n", COOLDOWN_N);
            for (int i = 0; i < COOLDOWN_N; i++) {
                if (i % 2 == 0) {
                    std::set<int64_t> result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i/2]);
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, warmup_attr[i/2], warmup_queries[i/2]);
                }
            }
            fprintf(stderr, "Cooldown complete.\n");

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
                    std::set<int64_t> result;
                    graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_size]);
                    if (result.size() == 0) {
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
                    std::set<int64_t> result;
                    graph->get_neighbors(result, neighbor_indices[i % size]);
                } else {
                    std::set<int64_t> result;
                    graph->search_nodes(result, queries_attr[i % size], queries[i % size]);
                }
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (double(1E6));
            }
            thput = ((double) MEASURE_N / totsecs);
            printf("Throughput: %f\n total queries: %lu, total time: %f\n\n", thput, MEASURE_N, totsecs);

            // Cooldown phase
            for (int i = 0; i < COOLDOWN_N; i++) {
                if (i % 2 == 0) {
                    std::set<int64_t> result;
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
