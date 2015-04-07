#ifndef SUCCINCT_GRAPH_NAME_BENCHMARK_H
#define SUCCINCT_GRAPH_NAME_BENCHMARK_H

#include <random>
#include "../../external/succinct-cpp/benchmark/include/Benchmark.hpp"
#include "../../include/succinct-graph/SuccinctGraph.hpp"

class NameBenchmark : public Benchmark {

private:
    static const count_t WARMUP_T = 60 * 1E6;
    static const count_t MEASURE_T = 120 * 1E6;
    static const count_t COOLDOWN_T = 30 * 1E6;

    void read_queries(std::string warmup_query_file, std::string query_file) {
        std::ifstream warmup_input(warmup_query_file);
        std::ifstream query_input(query_file);

        std::string line;
        while (getline(warmup_input, line)) {
            int split_pos = line.find(' ');
            int attr = std::atoi(line.substr(0, split_pos).c_str());
            std::string search = line.substr(split_pos + 1);
            warmup_attr.push_back(attr);
            warmup_queries.push_back(search);
        }
        while (getline(query_input, line)) {
            int split_pos = line.find(' ');
            int attr = std::atoi(line.substr(0, split_pos).c_str());
            std::string search = line.substr(split_pos + 1);
            queries_attr.push_back(attr);
            queries.push_back(search);
        }
        warmup_input.close();
        query_input.close();
    }

public:

    NameBenchmark(SuccinctGraph *graph, std::string warmup_query_file,
            std::string query_file) : Benchmark() {
        this->graph = graph;
        read_queries(warmup_query_file, query_file);
    }

    double benchmark_name_throughput() {
        double thput = 0;

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            std::cout << "Warming up" << std::endl;
            int warmup_size = warmup_queries.size();
            while (get_timestamp() - warmup_start < WARMUP_T) {
                std::set<int64_t> result;
                graph->search_nodes(result, warmup_attr[i % warmup_size], warmup_queries[i % warmup_size]);
                i++;
            }

            // Measure phase
            i = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            std::cout << "Measuring throughput" << std::endl;
            int size = queries.size();
            while (get_timestamp() - start < MEASURE_T) {
                std::set<int64_t> result;
                time_t query_start = get_timestamp();
                graph->search_nodes(result, queries_attr[i % size], queries[i % size]);
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (double(1E6));
                i++;
            }
            thput = ((double) i / totsecs);
            std::cout << "Throughput: " << thput << std::endl;

            i = 0;
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_T) {
                std::set<int64_t> result;
                graph->search_nodes(result, warmup_attr[i % warmup_size], warmup_queries[i % warmup_size]);
                i++;
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
    SuccinctGraph *graph;
};

#endif
