#ifndef SUCCINCT_GRAPH_NEIGHBOR_BENCHMARK_H
#define SUCCINCT_GRAPH_NEIGHBOR_BENCHMARK_H

#include <random>
#include "../../external/succinct-cpp/benchmark/include/Benchmark.hpp"
#include "../../include/succinct-graph/SuccinctGraph.hpp"

class NeighborBenchmark : public Benchmark {

private:
    static const count_t WARMUP_T = 60 * 1E6;
    static const count_t MEASURE_T = 120 * 1E6;
    static const count_t COOLDOWN_T = 30 * 1E6;

    void generate_query_files(std::string warmup_query_file, std::string query_file) {
        int64_t nodes = graph->num_nodes();
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int64_t> uni(0, nodes - 1);

        std::ofstream warmup_out(warmup_query_file);
        std::ofstream query_out(query_file);
        for(count_t i = 0; i < std::max(nodes, (int64_t)20000); i++) {
            int64_t rand_node = uni(rng);
            warmup_queries.push_back(rand_node);
            warmup_out << rand_node << std::endl;
        }

        for(count_t i = 0; i < std::max(nodes, (int64_t)50000); i++) {
            int64_t rand_node = uni(rng);
            queries.push_back(rand_node);
            query_out << rand_node << std::endl;
        }
        warmup_out.close();
        query_out.close();
        printf("Created files: %s, %s\n", warmup_query_file.c_str(), query_file.c_str());
    }

    void read_queries(std::string warmup_query_file, std::string query_file) {
        std::ifstream warmup_input(warmup_query_file);
        std::ifstream query_input(query_file);

        std::string line;
        while (getline(warmup_input, line)) {
            warmup_queries.push_back(std::strtoll(line.c_str(), NULL, 10));
        }

        while (getline(query_input, line)) {
            queries.push_back(std::strtoll(line.c_str(), NULL, 10));
        }
        warmup_input.close();
        query_input.close();
    }

public:

    NeighborBenchmark(SuccinctGraph *graph, std::string warmup_query_file,
            std::string query_file) : Benchmark() {
        this->graph = graph;
        std::ifstream query_input(query_file);
        if (query_input.good()) {
            read_queries(warmup_query_file, query_file);
        } else {
            generate_query_files(warmup_query_file, query_file);
        }
    }

    std::pair<double, double> benchmark_neighbor_throughput() {
        double get_neighbor_thput = 0;
        double edges_thput = 0;

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            while (get_timestamp() - warmup_start < WARMUP_T) {
                std::set<int64_t> result;
                graph->get_neighbors(result, warmup_queries[i % warmup_queries.size()]);
                i++;
            }

            // Measure phase
            i = 0;
            long edges = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            while (get_timestamp() - start < MEASURE_T) {
                std::set<int64_t> result;
                time_t query_start = get_timestamp();
                graph->get_neighbors(result, queries[i % queries.size()]);
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (double(1E6));
                edges += result.size();
                i++;
            }
            get_neighbor_thput = ((double) i / totsecs);
            edges_thput = ((double) edges / totsecs);

            i = 0;
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_T) {
                std::set<int64_t> result;
                graph->get_neighbors(result, warmup_queries[i % warmup_queries.size()]);
                i++;
            }

        } catch (std::exception &e) {
            fprintf(stderr, "Throughput test ends...\n");
        }

        return std::make_pair(get_neighbor_thput, edges_thput);
    }

private:
    std::vector<uint64_t> warmup_queries;
    std::vector<uint64_t> queries;
    SuccinctGraph *graph;
};

#endif
