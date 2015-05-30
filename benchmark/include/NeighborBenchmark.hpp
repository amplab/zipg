#ifndef SUCCINCT_GRAPH_NEIGHBOR_BENCHMARK_H
#define SUCCINCT_GRAPH_NEIGHBOR_BENCHMARK_H

#include "GraphBenchmark.hpp"
#include "../../include/succinct-graph/SuccinctGraph.hpp"

class NeighborBenchmark : public GraphBenchmark {
public:

    NeighborBenchmark(SuccinctGraph *graph, std::string warmup_query_file,
            std::string query_file) : GraphBenchmark() {
        this->graph = graph;
        read_neighbor_queries(warmup_query_file, query_file);
    }

    void benchmark_neighbor_latency(std::string res_path, count_t WARMUP_N, count_t MEASURE_N) {
        time_t t0, t1;
        std::ofstream res_stream(res_path);

        // Warmup
        fprintf(stderr, "Warming up for %lu queries...\n", WARMUP_N);
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            std::set<int64_t> result;
            this->graph->get_neighbors(result, warmup_neighbor_indices[i]);
            assert(result.size() != 0 && "No result found in benchmarking neighbor latency");
        }
        fprintf(stderr, "Warmup complete.\n");

        // Measure
        fprintf(stderr, "Measuring for %lu queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; i++) {
            std::set<int64_t> result;
            t0 = get_timestamp();
            this->graph->get_neighbors(result, neighbor_indices[i]);
            t1 = get_timestamp();
            assert(result.size() != 0 && "No result found in benchmarking node latency");
            res_stream << result.size() << "," << t1 - t0 << "\n";
        }
        fprintf(stderr, "Measure complete.\n");

        // Cooldown
        fprintf(stderr, "Cooling down for %lu queries...\n", COOLDOWN_N);
        for(uint64_t i = 0; i < COOLDOWN_N; i++) {
            std::set<int64_t> result;
            this->graph->get_neighbors(result, warmup_neighbor_indices[i]);
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
                std::set<int64_t> result;
                this->graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_neighbor_indices.size()]);
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
                this->graph->get_neighbors(result, neighbor_indices[i % neighbor_indices.size()]);
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
                std::set<int64_t> result;
                this->graph->get_neighbors(result, warmup_neighbor_indices[i % warmup_neighbor_indices.size()]);
                i++;
            }

        } catch (std::exception &e) {
            fprintf(stderr, "Throughput test ends...\n");
        }

        return std::make_pair(get_neighbor_thput, edges_thput);
    }

};

#endif
