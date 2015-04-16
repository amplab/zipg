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
        read_queries(warmup_query_file, query_file);
    }

    void benchmark_neighbor_latency(std::string res_path, count_t WARMUP_N, count_t MEASURE_N, count_t COOLDOWN_N) {
        time_t t0, t1;
        std::ofstream res_stream(res_path);

        // Warmup
        fprintf(stderr, "Warming up for %lu queries...\n", WARMUP_N);
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            std::string result;
            this->graph->get_neighbors(result, warmup_queries[i]);
            assert(result.length() != 0 && "No result found in benchmarking node latency");
        }
        fprintf(stderr, "Warmup complete.\n");

        // Measure
        fprintf(stderr, "Measuring for %lu queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; i++) {
            std::string result;
            t0 = get_timestamp();
            this->graph->get_neighbors(result, queries[i]);
            t1 = get_timestamp();
            assert(result.length() != 0 && "No result found in benchmarking node latency");
            double millisecs = (t1 - t0) / 1000.0;
            res_stream << queries[i] << "," << result.length() << "," << millisecs << "\n";
        }
        fprintf(stderr, "Measure complete.\n");

        // Cooldown
        fprintf(stderr, "Cooling down for %lu queries...\n", COOLDOWN_N);
        for(uint64_t i = 0; i < COOLDOWN_N; i++) {
            std::string result;
            this->graph->get_neighbors(result, warmup_queries[i]);
        }
        fprintf(stderr, "Cooldown complete.\n");

        res_stream.close();
    }

    std::pair<double, double> benchmark_neighbor_throughput() {
        double get_neighbor_thput = 0;
        double edges_thput = 0;

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            while (get_timestamp() - warmup_start < WARMUP_T) {
                std::string result;
                graph->get_neighbors(result, warmup_queries[i % warmup_queries.size()]);
                i++;
            }

            // Measure phase
            i = 0;
            long edges = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            while (get_timestamp() - start < MEASURE_T) {
                std::string result;
                time_t query_start = get_timestamp();
                graph->get_neighbors(result, queries[i % queries.size()]);
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (1E6);
                edges += result.size();
                i++;
            }
            get_neighbor_thput = ((double) i / totsecs);
            edges_thput = ((double) edges / totsecs);

            i = 0;
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_T) {
                std::string result;
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
