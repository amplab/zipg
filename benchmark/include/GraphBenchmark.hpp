#ifndef SUCCINCT_GRAPH_BENCHMARK_H
#define SUCCINCT_GRAPH_BENCHMARK_H

#include <string>
#include <sstream>
#include <vector>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "Benchmark.hpp"
#include "succinct-graph/SuccinctGraph.hpp"
#include "rpc/ports.h"
#include "succinct-graph/utils.h"
#include "thrift/GraphQueryAggregatorService.h"

using boost::shared_ptr;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

class GraphBenchmark : public Benchmark {
private:

    template<typename T>
    inline T mod_get(const std::vector<T>& xs, int i) {
        return xs[i % xs.size()];
    }

public:

    GraphBenchmark(SuccinctGraph *graph) : Benchmark() {
        graph_ = graph;

        if (graph_ == nullptr) {
            // sharded bench
            init_sharded_benchmark();

            get_neighbors_f_ = [=](std::vector<int64_t>& nhbrs, int64_t id) {
                aggregator_->get_neighbors(nhbrs, id);
            };

            get_neighbors_atype_f_ = [=](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int64_t atype)
            {
                aggregator_->get_neighbors_atype(nhbrs, id, atype);
            };

            get_neighbors_attr_f_ = [=](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int attr,
                const std::string& key)
            {
                aggregator_->get_neighbors_attr(nhbrs, id, attr, key);
            };

            get_nodes_f_ = [=](
                std::set<int64_t>& nodes,
                int attr,
                const std::string& key)
            {
                aggregator_->get_nodes(nodes, attr, key);
            };

            get_nodes2_f_ = [=](
                std::set<int64_t>& nodes,
                int attr1,
                const std::string& key1,
                int attr2,
                const std::string& key2)
            {
                aggregator_->get_nodes2(nodes, attr1, key1, attr2, key2);
            };
        } else {
            // not sharded, so call graph
            get_neighbors_f_ = [=](std::vector<int64_t>& nhbrs, int64_t id) {
                graph_->get_neighbors(nhbrs, id);
            };

            get_neighbors_atype_f_ = [=](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int64_t atype)
            {
                graph_->get_neighbors(nhbrs, id, atype);
            };

            get_neighbors_attr_f_ = [=](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int attr,
                const std::string& key)
            {
                graph_->get_neighbors(nhbrs, id, attr, key);
            };

            get_nodes_f_ = [=](
                std::set<int64_t>& nodes,
                int attr,
                const std::string& key)
            {
                graph_->get_nodes(nodes, attr, key);
            };

            get_nodes2_f_ = [=](
                std::set<int64_t>& nodes,
                int attr1,
                const std::string& key1,
                int attr2,
                const std::string& key2)
            {
                graph_->get_nodes(nodes, attr1, key1, attr2, key2);
            };
        }
    }

    void init_sharded_benchmark() {
        try {
            int port = QUERY_HANDLER_PORT;
            LOG_E("Connecting to server...\n");
            shared_ptr<TSocket> socket(new TSocket("localhost", port));
            shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
            shared_ptr<TProtocol> protocol(
                    new TBinaryProtocol(transport));
            aggregator_ = shared_ptr<GraphQueryAggregatorServiceClient>(
                new GraphQueryAggregatorServiceClient(protocol));
            transport->open();
            LOG_E("Connected to aggregator!\n");

            int ret = aggregator_->connect_to_local_shards();
            LOG_E("Aggregator connected to local shards, ret = %d\n", ret);

            aggregator_->init();
            LOG_E("Done init all shards\n");
        } catch (std::exception& e) {
            LOG_E("Exception in benchmark client: %s\n", e.what());
        }
    }

    // BENCHMARKING NEIGHBOR QUERIES
    void benchmark_neighbor_latency(
        std::string res_path,
        count_t WARMUP_N,
        count_t MEASURE_N,
        std::string warmup_query_file,
        std::string query_file)
    {
        time_t t0, t1;
        LOG_E("Benchmarking getNeighbor latency\n");
        read_neighbor_queries(warmup_query_file, query_file);
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        // Warmup
        LOG_E("Warming up for %lu queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for (uint64_t i = 0; i < WARMUP_N; i++) {
            get_neighbors_f_(result, mod_get(warmup_neighbor_indices, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %lu queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_neighbors_f_(result, mod_get(neighbor_indices, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "node id: " << mod_get(neighbor_indices, i) << "\n";
            std:sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it) {
                query_res_stream << *it << " ";
            }
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    // get_neighbor(nodeId, atype)
    void benchmark_neighbor_atype_latency(
        std::string res_path,
        count_t WARMUP_N,
        count_t MEASURE_N,
        std::string warmup_query_file,
        std::string query_file)
    {
        time_t t0, t1;
        LOG_E("Benchmarking getNeighborAtype latency\n");
        read_neighbor_atype_queries(warmup_query_file, query_file);
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        // Warmup
        LOG_E("Warming up for %lu queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for (uint64_t i = 0; i < WARMUP_N; i++) {
            get_neighbors_atype_f_(
                result,
                mod_get(warmup_neighbor_indices, i),
                mod_get(warmup_atypes, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %lu queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_neighbors_atype_f_(
                result,
                mod_get(neighbor_indices, i),
                mod_get(atypes, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "node id: " << mod_get(neighbor_indices, i) << "\n";
            query_res_stream << "atype:  " << mod_get(atypes, i) << "\n";
            std:sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it) {
                query_res_stream << *it << " ";
            }
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    std::pair<double, double> benchmark_neighbor_throughput(
            std::string warmup_query_file, std::string query_file) {
        double get_neighbor_thput = 0;
        double edges_thput = 0;
        read_neighbor_queries(warmup_query_file, query_file);

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            while (get_timestamp() - warmup_start < WARMUP_T) {
                std::vector<int64_t> result;
                get_neighbors_f_(result, mod_get(warmup_neighbor_indices, i));
                i++;
            }

            // Measure phase
            i = 0;
            long edges = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            while (get_timestamp() - start < MEASURE_T) {
                std::vector<int64_t> result;
                time_t query_start = get_timestamp();
                get_neighbors_f_(result, mod_get(neighbor_indices, i));
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (1E6);
                edges += result.size();
                i++;
            }
            get_neighbor_thput = ((double) i / totsecs);
            edges_thput = ((double) edges / totsecs);
        } catch (std::exception &e) {
            LOG_E("Throughput test ends...\n");
        }

        return std::make_pair(get_neighbor_thput, edges_thput);
    }

    // NODE BENCHMARKING
    void benchmark_node_latency(std::string res_path, count_t WARMUP_N, count_t MEASURE_N,
            std::string warmup_query_file, std::string query_file) {
        time_t t0, t1;
        read_node_queries(warmup_query_file, query_file);
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking getNode latency\n");

        // Warmup
        LOG_E("Warming up for %lu queries...\n", WARMUP_N);
        std::set<int64_t> result;
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            get_nodes_f_(
                result, mod_get(warmup_node_attributes, i), mod_get(warmup_node_queries, i));
            assert(result.size() != 0 && "No result found in benchmarking node latency");
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %lu queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_nodes_f_(result, mod_get(node_attributes, i), mod_get(node_queries, i));
            t1 = get_timestamp();
            assert(result.size() != 0 && "No result found in benchmarking node latency");
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "attr " << mod_get(node_attributes, i) << ": " << mod_get(node_queries, i) << "\n";
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " "; // sets are sorted
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_node_node_latency(std::string res_path, count_t WARMUP_N, count_t MEASURE_N,
            std::string warmup_query_file, std::string query_file) {
        read_node_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking getNode with two attributes latency\n");

        // Warmup
        LOG_E("Warming up for %lu queries...\n", WARMUP_N);
        std::set<int64_t> result;
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            get_nodes2_f_(result,
                mod_get(warmup_node_attributes, i), mod_get(warmup_node_queries, i),
                mod_get(warmup_node_attributes2, i), mod_get(warmup_node_queries2, i));
            assert(result.size() != 0 && "No result found in benchmarking node two attributes latency");
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %lu queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_nodes2_f_(result, mod_get(node_attributes, i), mod_get(node_queries, i),
                                              mod_get(node_attributes2, i), mod_get(node_queries2, i));
            t1 = get_timestamp();
            assert(result.size() != 0 && "No result found in benchmarking node two attributes latency");
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness
            query_res_stream << "attr1 " << mod_get(node_attributes, i) << ": " << mod_get(node_queries, i) << "; ";
            query_res_stream << "attr2 " << mod_get(node_attributes2, i) << ": " << mod_get(node_queries2, i) << "\n";
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " "; // sets are sorted
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    double benchmark_node_throughput(std::string warmup_query_file, std::string query_file) {
        double thput = 0;
        read_node_queries(warmup_query_file, query_file);

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            std::cout << "Warming up" << std::endl;
            while (get_timestamp() - warmup_start < WARMUP_T) {
                std::set<int64_t> result;
                get_nodes_f_(result,
                    mod_get(warmup_node_attributes, i), mod_get(warmup_node_queries, i));
                assert(result.size() != 0 && "No result found in benchmarking node throughput");
            }
            LOG_E("Warmup complete.\n");

            // Measure phase
            i = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            std::cout << "Measuring throughput" << std::endl;
            while (get_timestamp() - start < MEASURE_T) {
                std::set<int64_t> result;
                time_t query_start = get_timestamp();
                get_nodes_f_(result, mod_get(node_attributes, i), mod_get(node_queries, i));
                time_t query_end = get_timestamp();
                assert(result.size() != 0 && "No result found in benchmarking node throughput");
                totsecs += (double) (query_end - query_start) / (double(1E6));
                i++;
            }
            thput = ((double) i / totsecs);
            std::cout << "Throughput: " << thput << std::endl;
        } catch (std::exception &e) {
            LOG_E("Throughput test ends...\n");
        }

        return thput;
    }

    // BENCHMARKING MIX QUERIES
    void benchmark_mix_latency(std::string res_path, count_t WARMUP_N, count_t MEASURE_N,
            std::string warmup_neighbor_query_file, std::string neighbor_query_file,
            std::string warmup_node_query_file, std::string node_query_file) {
        std::ofstream res_stream(res_path);
        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file);
        read_node_queries(warmup_node_query_file, node_query_file);

        LOG_E("Benchmarking mixQuery latency\n");
        try {
            // Warmup phase
            LOG_E("Warming up for %lu queries...\n", WARMUP_N);
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    std::vector<int64_t> result;
                    get_neighbors_f_(result, mod_get(warmup_neighbor_indices, i / 2));
                    if (result.size() == 0) {
                        LOG_E("Error getting neighbors for %lld.\n",
                            mod_get(warmup_neighbor_indices, i / 2));
                    }
                } else {
                    std::set<int64_t> result;
                    get_nodes_f_(result,
                        mod_get(warmup_node_attributes, i / 2),
                        mod_get(warmup_node_queries, i / 2));
                    if (result.size() == 0) {
                        LOG_E("Error searching attr %d for %s\n",
                            mod_get(warmup_node_attributes, i / 2),
                            mod_get(warmup_node_queries, i / 2).c_str());
                    }
                }
            }
            LOG_E("Warmup complete.\n");

            // Measure phase
            LOG_E("Measuring for %lu queries...\n", MEASURE_N);
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 2 == 0) {
                    std::vector<int64_t> result;
                    time_t query_start = get_timestamp();
                    get_neighbors_f_(result, mod_get(neighbor_indices, i / 2));
                    time_t query_end = get_timestamp();
                    if (result.size() == 0) {
                        LOG_E(
                            "Error getting neighbors for %lld\n",
                            mod_get(neighbor_indices, i / 2));
                    } else {
                        res_stream << result.size() << "," <<  (query_end - query_start) << "\n";
                    }
                } else {
                    std::set<int64_t> result;
                    time_t query_start = get_timestamp();
                    get_nodes_f_(result,
                        mod_get(node_attributes, i / 2),
                        mod_get(node_queries, i / 2));
                    time_t query_end = get_timestamp();
                    if (result.size() == 0) {
                        printf("Error searching for attr %d for %s\n",
                            mod_get(node_attributes, i / 2),
                            mod_get(node_queries, i / 2).c_str());
                    } else {
                        res_stream << result.size() << "," <<  (query_end - query_start) << "\n";
                    }
                }
            }
            LOG_E("Measure complete.\n");
        } catch (std::exception &e) {
            LOG_E("Throughput test ends...\n");
        }
    }

    void benchmark_neighbor_node_latency(
        std::string res_path,
        count_t WARMUP_N,
        count_t MEASURE_N,
        std::string warmup_query_file,
        std::string query_file) {

        read_neighbor_node_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking getNeighborOfNode latency\n");

        // Warmup
        LOG_E("Warming up for %lu queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            get_neighbors_attr_f_(result,
                mod_get(warmup_neighbor_indices, i), mod_get(warmup_node_attributes, i), mod_get(warmup_node_queries, i));
            // assert(result.size() != 0 && "No result found in benchmarking getNeighborOfNode latency");
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %lu queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_neighbors_attr_f_(result,
                mod_get(neighbor_indices, i), mod_get(node_attributes, i), mod_get(node_queries, i));
            t1 = get_timestamp();
            // assert(result.size() != 0 && "No result found in benchmarking getNeighborOfNode latency");
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness
            query_res_stream << "id " << mod_get(neighbor_indices, i) << " attr " << mod_get(node_attributes, i);
            query_res_stream << " query " << mod_get(node_queries, i) << "\n";
            std::sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " ";
            query_res_stream << "\n";
#endif
        }

        LOG_E("Measure complete.\n");
    }

    double benchmark_mix_throughput(std::string warmup_neighbor_query_file, std::string neighbor_query_file,
            std::string warmup_node_query_file, std::string node_query_file) {
        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file);
        read_node_queries(warmup_node_query_file, node_query_file);

        double thput = 0;
        try {
            // Warmup phase
            std::cout << "Warming up" << std::endl;
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    std::vector<int64_t> result;
                    get_neighbors_f_(result, mod_get(warmup_neighbor_indices, i / 2));
                    if (result.size() == 0) {
                        LOG_E(
                            "Error getting neighbors for %lld\n",
                            mod_get(warmup_neighbor_indices, i / 2));
                        std::exit(1);
                    }
                } else {
                    std::set<int64_t> result;
                    get_nodes_f_(result,
                        mod_get(warmup_node_attributes, i / 2),
                        mod_get(warmup_node_queries, i / 2));
                    if (result.size() == 0) {
                        printf("Error searching for attr %d for %s\n",
                            mod_get(warmup_node_attributes, i / 2),
                            mod_get(warmup_node_queries, i / 2).c_str());
                        std::exit(1);
                    }
                }
            }

            // Measure phase
            double totsecs = 0;
            std::cout << "Measuring throughput" << std::endl;
            for (int i = 0; i < MEASURE_N; i++) {
                time_t query_start = get_timestamp();
                if (i % 2 == 0) {
                    std::vector<int64_t> result;
                    get_neighbors_f_(result, mod_get(neighbor_indices, i / 2));
                } else {
                    std::set<int64_t> result;
                    get_nodes_f_(result, mod_get(node_attributes, i / 2), mod_get(node_queries, i / 2));
                }
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (double(1E6));
            }
            thput = ((double) MEASURE_N / totsecs);
            printf("Throughput: %f\n total queries: %lu, total time: %f\n\n", thput, MEASURE_N, totsecs);
        } catch (std::exception &e) {
            LOG_E("Throughput test ends...\n");
        }

        return thput;
    }


protected:

    SuccinctGraph * graph_;
    shared_ptr<GraphQueryAggregatorServiceClient> aggregator_;

    std::function<void(std::vector<int64_t>&, int64_t)> get_neighbors_f_;

    std::function<void(
        std::vector<int64_t>&,
        int64_t,
        int64_t)> get_neighbors_atype_f_;

    std::function<void(
        std::vector<int64_t>&,
        int64_t,
        int,
        const std::string&)> get_neighbors_attr_f_;

    std::function<void(
        std::set<int64_t>&,
        int,
        const std::string&)> get_nodes_f_;

    std::function<void(
        std::set<int64_t>&,
        int,
        const std::string&,
        int,
        const std::string&)> get_nodes2_f_;

    count_t WARMUP_N; count_t MEASURE_N;
    static const count_t COOLDOWN_N = 500;

    std::vector<int64_t> warmup_neighbor_indices, neighbor_indices;

    std::vector<int> warmup_atypes, atypes;

    std::vector<int> warmup_node_attributes;
    std::vector<std::string> warmup_node_queries;
    std::vector<int> node_attributes;
    std::vector<std::string> node_queries;
    std::vector<int> warmup_node_attributes2;
    std::vector<std::string> warmup_node_queries2;
    std::vector<int> node_attributes2;
    std::vector<std::string> node_queries2;

    // READING QUERY FILES
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
    }

    void read_neighbor_atype_queries(
        std::string warmup_file, std::string query_file) {

        std::ifstream warmup_input(warmup_file);
        std::ifstream query_input(query_file);
        std::string line;
        while (getline(warmup_input, line)) {
            std::vector<std::string> toks = split(line, ',');
            warmup_neighbor_indices.push_back(std::stol(toks[0]));
            warmup_atypes.push_back(std::stoi(toks[1]));
        }
        while (getline(query_input, line)) {
            std::vector<std::string> toks = split(line, ',');
            neighbor_indices.push_back(std::stol(toks[0]));
            atypes.push_back(std::stoi(toks[1]));
        }
    }

    void read_node_queries(std::string warmup_query_file, std::string query_file) {
        std::ifstream warmup_input(warmup_query_file);
        std::ifstream query_input(query_file);

        std::string line;
        while (getline(warmup_input, line)) {
            std::vector<std::string> toks = split(
                line, GraphFormatter::QUERY_FILED_DELIM);
            warmup_node_attributes.push_back(std::atoi(toks[0].c_str()));
            warmup_node_queries.push_back(toks[1]);
            warmup_node_attributes2.push_back(std::atoi(toks[2].c_str()));
            warmup_node_queries2.push_back(toks[3]);
        }
        while (getline(query_input, line)) {
            std::vector<std::string> toks = split(
                line, GraphFormatter::QUERY_FILED_DELIM);
            node_attributes.push_back(std::atoi(toks[0].c_str()));
            node_queries.push_back(toks[1]);
            node_attributes2.push_back(std::atoi(toks[2].c_str()));
            node_queries2.push_back(toks[3]);
        }
    }

    void read_neighbor_node_queries(std::string warmup_query_file, std::string query_file) {
        std::ifstream warmup_input(warmup_query_file);
        std::ifstream query_input(query_file);

        std::string line;
        while (getline(warmup_input, line)) {
            // Format: nodeId,attrId,[everything to EOL is attr]
            // Since attr can contain ',', we don't use split() to parse
            int pos = line.find(',');
            warmup_neighbor_indices.push_back(std::stoi(line.substr(0, pos)));
            int pos2 = line.find(',', pos + 1);
            warmup_node_attributes.push_back(std::stoi(line.substr(pos + 1, pos2 - pos - 1)));
            warmup_node_queries.push_back(line.substr(pos2 + 1));
        }
        while (getline(query_input, line)) {
            int pos = line.find(',');
            neighbor_indices.push_back(std::stoi(line.substr(0, pos)));
            int pos2 = line.find(',', pos + 1);
            node_attributes.push_back(std::stoi(line.substr(pos + 1, pos2 - pos - 1)));
            node_queries.push_back(line.substr(pos2 + 1));
        }
    }

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
