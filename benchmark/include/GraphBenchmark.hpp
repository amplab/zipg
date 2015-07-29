#ifndef SUCCINCT_GRAPH_BENCHMARK_H
#define SUCCINCT_GRAPH_BENCHMARK_H

#include <random>
#include <string>
#include <sstream>
#include <thread>
#include <vector>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "succinct-graph/SuccinctGraph.hpp"
#include "rpc/ports.h"
#include "succinct-graph/utils.h"
#include "thrift/GraphQueryAggregatorService.h"

using boost::shared_ptr;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

class GraphBenchmark {
private:

    template<typename T>
    inline T mod_get(const std::vector<T>& xs, int i) {
        return xs[i % xs.size()];
    }

    typedef struct {
        shared_ptr<GraphQueryAggregatorServiceClient> client;
        // get_nhbrs(n)
        std::vector<int64_t> warmup_neighbor_indices, neighbor_indices;

//        // get_nhbrs(n, atype)
//        std::vector<int64_t> warmup_nhbrAtype_indices, nhbrAtype_indices;
//        std::vector<int> warmup_atypes, atypes;
//
//        // get_nhbrs(n, attr)
//        std::vector<int64_t> warmup_nhbrNode_indices, nhbrNode_indices;
//        std::vector<int> warmup_nhbrNode_attr_ids, nhbrNode_attr_ids;
//        std::vector<std::string> warmup_nhbrNode_attrs, nhbrNode_attrs;
//
//        // 2 get_nodes()
//        std::vector<int> warmup_node_attributes, node_attributes;
//        std::vector<std::string> warmup_node_queries, node_queries;
//        std::vector<int> warmup_node_attributes2, node_attributes2;
//        std::vector<std::string> warmup_node_queries2, node_queries2;
    } benchmark_thread_data_t;

public:

    GraphBenchmark(SuccinctGraph *graph) {
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
        uint64_t WARMUP_N,
        uint64_t MEASURE_N,
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
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for (uint64_t i = 0; i < WARMUP_N; i++) {
            get_neighbors_f_(result, mod_get(warmup_neighbor_indices, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_neighbors_f_(result, mod_get(neighbor_indices, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "node id: " << mod_get(neighbor_indices, i)
                << "\n";
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
        uint64_t WARMUP_N,
        uint64_t MEASURE_N,
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
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for (uint64_t i = 0; i < WARMUP_N; i++) {
            get_neighbors_atype_f_(
                result,
                mod_get(warmup_neighbor_indices, i),
                mod_get(warmup_atypes, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
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
            query_res_stream << "node id: " << mod_get(neighbor_indices, i)
                << "\n";
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

    std::pair<double, double> benchmark_neighbor_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data,
        int64_t warmup_microsecs,
        int64_t measure_microsecs,
        int64_t cooldown_microsecs)
    {
        double query_thput = 0;
        double edges_thput = 0;

        try {
            std::vector<int64_t> result;

            // Warmup phase
            int64_t i = 0;
            time_t start = get_timestamp();
            while (get_timestamp() - start < warmup_microsecs) {
                thread_data->client->get_neighbors(
                    result, mod_get(thread_data->warmup_neighbor_indices, i));
                ++i;
            }
            LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < measure_microsecs) {
                thread_data->client->get_neighbors(
                    result, mod_get(thread_data->neighbor_indices, i));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            LOG_E("Query done: served %" PRId64 " queries\n", i);

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < cooldown_microsecs) {
                thread_data->client->get_neighbors(
                    result, mod_get(thread_data->neighbor_indices, i));
            }

            LOG_E("query throughput %.1f\nedge throughput %.1f\n",
                query_thput, edges_thput);
            std::ofstream ofs("throughput_get_nhbrs.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput;

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    void benchmark_neighbor_throughput(
        int num_threads,
        int64_t warmup_microsecs,
        int64_t measure_microsecs,
        int64_t cooldown_microsecs,
        const std::string& warmup_neighbor_query_file,
        const std::string& neighbor_query_file)
    {
        std::vector<shared_ptr<benchmark_thread_data_t>> thread_datas;
        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file);
        for (int i = 0; i < num_threads; ++i) {
            try {
                shared_ptr<TSocket> socket(
                    new TSocket("localhost", QUERY_HANDLER_PORT));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                shared_ptr<GraphQueryAggregatorServiceClient> client(
                    new GraphQueryAggregatorServiceClient(protocol));

                transport->open();
                client->connect_to_local_shards();
                client->init();

                shared_ptr<benchmark_thread_data_t> thread_data(
                    new benchmark_thread_data_t);
                thread_data->client = client;

                // shuffle & copy-assign the queries for each thread
                std::srand(1618 + i);
                std::random_shuffle(
                    warmup_neighbor_indices.begin(),
                    warmup_neighbor_indices.end());
                std::random_shuffle(
                    neighbor_indices.begin(),
                    neighbor_indices.end());
                thread_data->warmup_neighbor_indices = warmup_neighbor_indices;
                thread_data->neighbor_indices = neighbor_indices;

                thread_datas.push_back(thread_data);

            } catch (std::exception& e) {
                LOG_E("Exception opening clients: %s\n", e.what());
            }
        }

        std::vector<shared_ptr<std::thread>> threads;
        for (auto thread_data : thread_datas) {
            shared_ptr<std::thread> thread(new std::thread(
                &GraphBenchmark::benchmark_neighbor_throughput_helper,
                this,
                thread_data,
                warmup_microsecs,
                measure_microsecs,
                cooldown_microsecs));
            threads.push_back(thread);
        }

        for (auto thread : threads) {
            thread->join();
        }
    }

    // NODE BENCHMARKING
    void benchmark_node_latency(
        std::string res_path,
        uint64_t WARMUP_N,
        uint64_t MEASURE_N,
        std::string warmup_query_file,
        std::string query_file)
    {
        time_t t0, t1;
        read_node_queries(warmup_query_file, query_file);
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking getNode latency\n");

        // Warmup
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::set<int64_t> result;
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            get_nodes_f_(
                result, mod_get(warmup_node_attributes, i),
                mod_get(warmup_node_queries, i));
            assert(result.size() != 0 && "No result found in"
                " benchmarking node latency");
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_nodes_f_(result, mod_get(node_attributes, i),
                mod_get(node_queries, i));
            t1 = get_timestamp();
            assert(result.size() != 0 && "No result found in"
                " benchmarking node latency");
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "attr " << mod_get(node_attributes, i) << ": "
                << mod_get(node_queries, i) << "\n";
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " "; // sets are sorted
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_node_node_latency(
        std::string res_path,
        uint64_t WARMUP_N,
        uint64_t MEASURE_N,
        std::string warmup_query_file,
        std::string query_file)
    {
        read_node_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking getNode with two attributes latency\n");

        // Warmup
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::set<int64_t> result;
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            get_nodes2_f_(result,
                mod_get(warmup_node_attributes, i),
                mod_get(warmup_node_queries, i),
                mod_get(warmup_node_attributes2, i),
                mod_get(warmup_node_queries2, i));
            assert(result.size() != 0 && "No result found in benchmarking node"
                " two attributes latency");
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_nodes2_f_(result, mod_get(node_attributes, i),
                mod_get(node_queries, i), mod_get(node_attributes2, i),
                mod_get(node_queries2, i));
            t1 = get_timestamp();
            assert(result.size() != 0 && "No result found in benchmarking node"
                " two attributes latency");
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness
            query_res_stream << "attr1 " << mod_get(node_attributes, i) << ": "
                << mod_get(node_queries, i) << "; ";
            query_res_stream << "attr2 " << mod_get(node_attributes2, i) << ": "
                << mod_get(node_queries2, i) << "\n";
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " "; // sets are sorted
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    // BENCHMARKING MIX QUERIES
    void benchmark_mix_latency(
        const std::string& nhbr_res_file,
        const std::string& nhbr_atype_res_file,
        const std::string& nhbr_node_res_file,
        const std::string& node_res_file,
        const std::string& node_node_res_file,
        uint64_t WARMUP_N, uint64_t MEASURE_N,
        const std::string& warmup_neighbor_query_file,
        const std::string& neighbor_query_file,
        const std::string& warmup_nhbr_atype_file,
        const std::string& nhbr_atype_file,
        const std::string& warmup_nhbr_node_file,
        const std::string& nhbr_node_file,
        const std::string& warmup_node_query_file,
        const std::string& node_query_file)
    {
        std::ofstream nhbr_res(nhbr_res_file);
        std::ofstream nhbr_atype_res(nhbr_atype_res_file);
        std::ofstream nhbr_node_res(nhbr_node_res_file);
        std::ofstream node_res(node_res_file);
        std::ofstream node_node_res(node_node_res_file);

        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file);
        read_neighbor_atype_queries(warmup_nhbr_atype_file, nhbr_atype_file);
        read_neighbor_node_queries(warmup_nhbr_node_file, nhbr_node_file);
        read_node_queries(warmup_node_query_file, node_query_file);

        std::mt19937 rng(1618);
        std::uniform_int_distribution<int> uni(0, 4);

        std::vector<int64_t> result;
        std::set<int64_t> result_set;

        LOG_E("Benchmarking mixQuery latency\n");
        try {
            // Warmup phase
            LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
            for (int i = 0; i < WARMUP_N; ++i) {
                int rand_query = uni(rng);
                switch (rand_query) {
                case 0:
                    get_neighbors_f_(result,
                        mod_get(warmup_neighbor_indices, i));
                    break;
                case 1:
                    get_neighbors_attr_f_(result,
                        mod_get(warmup_nhbrNode_indices, i),
                        mod_get(warmup_nhbrNode_attr_ids, i),
                        mod_get(warmup_nhbrNode_attrs, i));
                    break;
                case 2:
                    get_nodes_f_(result_set,
                        mod_get(warmup_node_attributes, i),
                        mod_get(warmup_node_queries, i));
                    break;
                case 3:
                    get_neighbors_atype_f_(result,
                        mod_get(warmup_nhbrAtype_indices, i),
                        mod_get(warmup_atypes, i));
                    break;
                case 4:
                    get_nodes2_f_(result_set,
                        mod_get(warmup_node_attributes, i),
                        mod_get(warmup_node_queries, i),
                        mod_get(warmup_node_attributes2, i),
                        mod_get(warmup_node_queries2, i));
                    break;
                default:
                    assert(false);
                }
            }
            LOG_E("Warmup complete.\n");

            rng.seed(1618);

            // Measure phase
            LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
            int64_t latency = 0;
            for (int i = 0; i < MEASURE_N; ++i) {
                int rand_query = uni(rng);
                switch (rand_query) {
                case 0:
                {
                    scoped_timer t(&latency);
                    get_neighbors_f_(result, mod_get(neighbor_indices, i));
                }
                    nhbr_res << result.size() << "," << latency << std::endl;
                    break;
                case 1:
                {
                    scoped_timer t(&latency);
                    get_neighbors_attr_f_(result,
                        mod_get(nhbrNode_indices, i),
                        mod_get(nhbrNode_attr_ids, i),
                        mod_get(nhbrNode_attrs, i));
                }
                    nhbr_node_res << result.size() << "," << latency << "\n";
                    break;
                case 2:
                {
                    scoped_timer t(&latency);
                    get_nodes_f_(result_set,
                        mod_get(node_attributes, i),
                        mod_get(node_queries, i));
                }
                    node_res << result_set.size() << "," << latency << "\n";
                    break;
                case 3:
                {
                    scoped_timer t(&latency);
                    get_neighbors_atype_f_(result,
                        mod_get(nhbrAtype_indices, i),
                        mod_get(atypes, i));
                }
                    nhbr_atype_res << result.size() << "," << latency << "\n";
                    break;
                case 4:
                {
                    scoped_timer t(&latency);
                    get_nodes2_f_(result_set,
                        mod_get(node_attributes, i),
                        mod_get(node_queries, i),
                        mod_get(node_attributes2, i),
                        mod_get(node_queries2, i));
                }
                    node_node_res << result_set.size() << "," << latency
                    << "\n";
                    break;
                default:
                    assert(false);
                }
            }
            LOG_E("Measure complete.\n");
        } catch (std::exception &e) {
            LOG_E("Exception: %s\n", e.what());
        }
    }

    void benchmark_neighbor_node_latency(
        std::string res_path,
        uint64_t WARMUP_N,
        uint64_t MEASURE_N,
        std::string warmup_query_file,
        std::string query_file)
    {
        read_neighbor_node_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking getNeighborOfNode latency\n");

        // Warmup
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for(uint64_t i = 0; i < WARMUP_N; i++) {
            get_neighbors_attr_f_(result,
                mod_get(warmup_neighbor_indices, i),
                mod_get(warmup_node_attributes, i),
                mod_get(warmup_node_queries, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; i++) {
            t0 = get_timestamp();
            get_neighbors_attr_f_(result,
                mod_get(neighbor_indices, i),
                mod_get(node_attributes, i),
                mod_get(node_queries, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness
            query_res_stream << "id " << mod_get(neighbor_indices, i)
                << " attr " << mod_get(node_attributes, i);
            query_res_stream << " query " << mod_get(node_queries, i) << "\n";
            std::sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " ";
            query_res_stream << "\n";
#endif
        }

        LOG_E("Measure complete.\n");
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

    uint64_t WARMUP_N; uint64_t MEASURE_N;
    static const uint64_t COOLDOWN_N = 500;

    // get_nhbrs(n)
    std::vector<int64_t> warmup_neighbor_indices, neighbor_indices;

    // get_nhbrs(n, atype)
    std::vector<int64_t> warmup_nhbrAtype_indices, nhbrAtype_indices;
    std::vector<int> warmup_atypes, atypes;

    // get_nhbrs(n, attr)
    std::vector<int64_t> warmup_nhbrNode_indices, nhbrNode_indices;
    std::vector<int> warmup_nhbrNode_attr_ids, nhbrNode_attr_ids;
    std::vector<std::string> warmup_nhbrNode_attrs, nhbrNode_attrs;

    // 2 get_nodes()
    std::vector<int> warmup_node_attributes, node_attributes;
    std::vector<std::string> warmup_node_queries, node_queries;
    std::vector<int> warmup_node_attributes2, node_attributes2;
    std::vector<std::string> warmup_node_queries2, node_queries2;

    // READING QUERY FILES
    void read_neighbor_queries(
        const std::string& warmup_neighbor_file,
        const std::string& query_neighbor_file)
    {
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
        const std::string& warmup_file,
        const std::string& query_file)
    {
        std::ifstream warmup_input(warmup_file);
        std::ifstream query_input(query_file);
        std::string line;
        while (getline(warmup_input, line)) {
            std::vector<std::string> toks = split(line, ',');
            warmup_nhbrAtype_indices.push_back(std::stoll(toks[0]));
            warmup_atypes.push_back(std::stoi(toks[1]));
        }
        while (getline(query_input, line)) {
            std::vector<std::string> toks = split(line, ',');
            nhbrAtype_indices.push_back(std::stoll(toks[0]));
            atypes.push_back(std::stoi(toks[1]));
        }
    }

    void read_node_queries(
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
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

    void read_neighbor_node_queries(
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        std::ifstream warmup_input(warmup_query_file);
        std::ifstream query_input(query_file);

        std::string line;
        while (getline(warmup_input, line)) {
            // Format: nodeId,attrId,[everything to EOL is attr]
            // Since attr can contain ',', we don't use split() to parse
            int pos = line.find(',');
            warmup_nhbrNode_indices.push_back(std::stoll(line.substr(0, pos)));
            int pos2 = line.find(',', pos + 1);
            warmup_nhbrNode_attr_ids.push_back(
                std::stoi(line.substr(pos + 1, pos2 - pos - 1)));
            warmup_nhbrNode_attrs.push_back(line.substr(pos2 + 1));
        }
        while (getline(query_input, line)) {
            int pos = line.find(',');
            nhbrNode_indices.push_back(std::stoll(line.substr(0, pos)));
            int pos2 = line.find(',', pos + 1);
            nhbrNode_attr_ids.push_back(
            std::stoi(line.substr(pos + 1, pos2 - pos - 1)));
            nhbrNode_attrs.push_back(line.substr(pos2 + 1));
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
