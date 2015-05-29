#ifndef SUCCINCT_GRAPH_BENCHMARK_H
#define SUCCINCT_GRAPH_BENCHMARK_H

#include <string>
#include <sstream>
#include <vector>
#include "../../external/succinct-cpp/benchmark/include/Benchmark.hpp"
#include "../../include/succinct-graph/SuccinctGraph.hpp"

class GraphBenchmark : public Benchmark {
protected:
    count_t WARMUP_N; count_t MEASURE_N;
    static const count_t COOLDOWN_N = 500;

    std::vector<int> warmup_neighbor_indices;
    std::vector<int> neighbor_indices;

    std::vector<int> warmup_node_attributes;
    std::vector<std::string> warmup_node_queries;
    std::vector<int> node_attributes;
    std::vector<std::string> node_queries;

    void read_neighbor_queries(std::string warmup_neighbor_file, std::string query_neighbor_file) {
        std::ifstream warmup_input(warmup_neighbor_file);
        std::ifstream query_input(query_neighbor_file);

        std::string line;
        while (getline(warmup_input, line)) {
            warmup_neighbor_indices.push_back(std::atoi(line.c_str()));
        }

        while (getline(query_input, line)) {
            neighbor_indices.push_back(std::atoi(line.c_str()));
        }
        warmup_input.close();
        query_input.close();
    }

    void read_node_queries(std::string warmup_query_file, std::string query_file) {
        std::ifstream warmup_input(warmup_query_file);
        std::ifstream query_input(query_file);

        std::string line;
        while (getline(warmup_input, line)) {
            std::vector<std::string> toks = split(line, ',');
            warmup_node_attributes.push_back(std::atoi(toks[0].c_str()));
            warmup_node_queries.push_back(toks[1]);
        }
        while (getline(query_input, line)) {
            std::vector<std::string> toks = split(line, ',');
            node_attributes.push_back(std::atoi(toks[0].c_str()));
            node_queries.push_back(toks[1]);
        }
        warmup_input.close();
        query_input.close();
    }

private:
    std::vector<std::string> split(const std::string &s, char delim) {
        std::vector<std::string> elems;
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, delim)) {
            elems.push_back(item);
        }
        return elems;
    }
};

#endif
