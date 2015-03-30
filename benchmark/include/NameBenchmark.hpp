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

    void generate_query_files(std::string warmup_query_file, std::string query_file) {
        int64_t nodes = graph->num_nodes();
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int64_t> uni(0, nodes - 1);
        std::string name;

        std::ofstream warmup_out(warmup_query_file);
        std::ofstream query_out(query_file);
        for(count_t i = 0; i < std::max(nodes, (int64_t)20000); i++) {
            this->graph->get_name(name, uni(rng));
            warmup_queries.push_back(name);
            warmup_out << name << std::endl;
        }

        for(count_t i = 0; i < std::max(nodes, (int64_t)50000); i++) {
            this->graph->get_name(name, uni(rng));
            queries.push_back(name);
            query_out << name << std::endl;
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
            warmup_queries.push_back(line);
        }
        while (getline(query_input, line)) {
            queries.push_back(line);
        }
        warmup_input.close();
        query_input.close();
    }

public:

    NameBenchmark(SuccinctGraph *graph, std::string warmup_query_file,
            std::string query_file) : Benchmark() {
        this->graph = graph;
        std::ifstream query_input(query_file);
        if(query_input.good()) {
            read_queries(warmup_query_file, query_file);
        } else {
            generate_query_files(warmup_query_file, query_file);
        }
    }

    double benchmark_name_throughput() {
        double thput = 0;
        std::set<int64_t> result;

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            std::cout << "Warming up" << std::endl;
            while (get_timestamp() - warmup_start < WARMUP_T) {
                graph->get_nodes(result, warmup_queries[i % warmup_queries.size()]);
                i++;
            }

            // Measure phase
            i = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            std::cout << "Measuring throughput" << std::endl;
            while (get_timestamp() - start < MEASURE_T) {
                time_t query_start = get_timestamp();
                graph->get_nodes(result, queries[i % queries.size()]);
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (double(1E6));
                i++;
            }
            thput = ((double) i / totsecs);
            std::cout << "Throughput: " << thput << std::endl;

            i = 0;
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_T) {
                graph->get_nodes(result, warmup_queries[i % warmup_queries.size()]);
                i++;
            }

        } catch (std::exception &e) {
            fprintf(stderr, "Throughput test ends...\n");
        }

        return thput;
    }

private:
    std::vector<std::string> warmup_queries;
    std::vector<std::string> queries;
    SuccinctGraph *graph;
};

#endif
