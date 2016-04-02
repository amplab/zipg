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

#include "SuccinctGraph.hpp"
#include "ports.h"
#include "utils.h"
#include "GraphQueryAggregatorService.h"

using boost::shared_ptr;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

class GraphBenchmark {
private:

    // FIXME: these are hard-coded hacks.
    // Twitter
    constexpr static int64_t NUM_NODES = 41652230;
    constexpr static int64_t NUM_ATYPES = 5;
    constexpr static int64_t MIN_TIME = 1439721981221; // unused
    constexpr static int64_t MAX_TIME = 1441905687237;
    const std::string ATTR_FOR_NEW_EDGES = std::string(128, '|');

    const static int MAX_NUM_NEW_EDGES = 200000; // 3.5M takes too long to run

    // Timings for throughput benchmarks.
    constexpr static int64_t WARMUP_MICROSECS = 300 * 1000 * 1000;
    constexpr static int64_t MEASURE_MICROSECS = 900 * 1000 * 1000;
    constexpr static int64_t COOLDOWN_MICROSECS = 450 * 1000 * 1000;
    // constexpr static int64_t WARMUP_MICROSECS = 30 * 1000 * 1000;
    // constexpr static int64_t MEASURE_MICROSECS = 120 * 1000 * 1000;
    // constexpr static int64_t COOLDOWN_MICROSECS = 30 * 1000 * 1000;

    constexpr static int query_batch_size = 100;

    typedef enum {
        NHBR = 0,
        NHBR_ATYPE = 1,
        NHBR_NODE = 2,
        NODE = 3, // deprecated
        NODE2 = 4,
        MIX = 5,
        TAO_MIX = 6,
        EDGE_ATTRS = 7,
        TAO_MIX_WITH_UPDATES = 8
    } BenchType;

    // Read workload distribution; from ATC 13 Bronson et al.
    constexpr static double TAO_WRITE_PERC = 0.002;
    constexpr static double ASSOC_RANGE_PERC = 0.409;
    constexpr static double OBJ_GET_PERC = 0.289;
    constexpr static double ASSOC_GET_PERC = 0.157;
    constexpr static double ASSOC_COUNT_PERC = 0.117;
    constexpr static double ASSOC_TIME_RANGE_PERC = 0.028;

    inline int choose_query(double rand_r) {
        if (rand_r < ASSOC_RANGE_PERC) {
            return 0;
        } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
            return 1;
        } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
            return 2;
        } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC +
            ASSOC_GET_PERC + ASSOC_COUNT_PERC)
        {
            return 3;
        }
        return 4;
    }

    inline int choose_query_with_updates(double rand_update, double rand_r) {
        if (rand_update < TAO_WRITE_PERC) {
            return 5; // assoc_add only, for now
        }
        // otherwise, all masses are allocated to reads
        if (rand_r < ASSOC_RANGE_PERC) {
            return 0;
        } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
            return 1;
        } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
            return 2;
        } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC +
            ASSOC_GET_PERC + ASSOC_COUNT_PERC)
        {
            return 3;
        }
        return 4;
    }

    void bench_throughput(
        const int num_threads,
        const std::string& master_hostname,
        const BenchType type)
    {
        std::vector<shared_ptr<benchmark_thread_data_t>> thread_datas;
        for (int i = 0; i < num_threads; ++i) {
            try {
                shared_ptr<TSocket> socket(
                    new TSocket(master_hostname, QUERY_HANDLER_PORT));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                shared_ptr<GraphQueryAggregatorServiceClient> client(
                    new GraphQueryAggregatorServiceClient(protocol));
                transport->open();
                client->init();

                shared_ptr<benchmark_thread_data_t> thread_data(
                    new benchmark_thread_data_t);
                thread_data->client = client;
                thread_data->client_id = i;
                thread_data->transport = transport;
                thread_data->master_hostname = master_hostname;

                thread_datas.push_back(thread_data);

            } catch (std::exception& e) {
                LOG_E("Exception opening clients: %s\n", e.what());
            }
        }

        std::vector<shared_ptr<std::thread>> threads;
        time_t start;

        switch (type) {
        case NHBR:
            LOG_E("Starting nhbr thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_neighbor_throughput_helper,
                    this, thread_data)));
            }
            break;
        case NHBR_ATYPE:
            LOG_E("Starting nhbrAtype thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_neighbor_atype_throughput_helper,
                    this, thread_data)));
            }
            break;
        case NHBR_NODE:
            LOG_E("Starting nhbrAttr thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_neighbor_node_throughput_helper,
                    this, thread_data)));
            }
            break;
        case NODE:
            LOG_E("Starting node thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_node_throughput_helper,
                    this, thread_data)));
            }
            break;
        case NODE2:
            LOG_E("Starting nodeNode thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_node_node_throughput_helper,
                    this, thread_data)));
            }
            break;
        case MIX:
            LOG_E("Starting mix thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_mix_throughput_helper,
                    this, thread_data)));
            }
            break;
        case TAO_MIX:
            LOG_E("Starting taoMix thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_tao_mix_throughput_helper,
                    this, thread_data)));
            }
            break;
        case EDGE_ATTRS:
            LOG_E("Starting edgeAttrs thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_edge_attrs_throughput_helper,
                    this, thread_data)));
            }
            break;
        case TAO_MIX_WITH_UPDATES:
            LOG_E("Starting taoMixWithUpdates thput\n");
            for (auto thread_data : thread_datas) {
                threads.push_back(shared_ptr<std::thread>(new std::thread(
                    &GraphBenchmark::benchmark_tao_mix_with_updates_throughput_helper,
                    this, thread_data)));
            }
            break;
        default:
            assert(false);
        }

        start = get_timestamp();
        for (auto thread : threads) {
            thread->join();
        }
        LOG_E("Ends thput,%.1f secs [warm+measure+cool = %.1f]\n",
            (get_timestamp() - start) * 1. / 1e6,
            (WARMUP_MICROSECS + MEASURE_MICROSECS + COOLDOWN_MICROSECS) / 1e6);

        // Close the client-side transports, otherwise Thrift on the
        // server-side doesn't place nicely with next connections.
        for (auto thread_data : thread_datas) {
            thread_data->transport->close();
        }
        thread_datas.clear();
    }

    template<typename T>
    inline T mod_get(const std::vector<T>& xs, int64_t i) {
        return xs[i % xs.size()];
    }

    typedef struct {
        shared_ptr<GraphQueryAggregatorServiceClient> client;
        shared_ptr<TTransport> transport;
        std::string master_hostname;
        int client_id; // for seeding
    } benchmark_thread_data_t;

public:

    GraphBenchmark(SuccinctGraph *graph, const std::string& master_hostname) {
        graph_ = graph;

        if (graph_ == nullptr) {
            // sharded bench
            init_sharded_benchmark(master_hostname);

            get_neighbors_f_ = [this](std::vector<int64_t>& nhbrs, int64_t id) {
                aggregator_->get_neighbors(nhbrs, id);
            };

            get_neighbors_atype_f_ = [this](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int64_t atype)
            {
                aggregator_->get_neighbors_atype(nhbrs, id, atype);
            };

            get_neighbors_attr_f_ = [this](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int attr,
                const std::string& key)
            {
                aggregator_->get_neighbors_attr(nhbrs, id, attr, key);
            };

            get_nodes_f_ = [this](
                std::set<int64_t>& nodes,
                int attr,
                const std::string& key)
            {
                aggregator_->get_nodes(nodes, attr, key);
            };

            get_nodes2_f_ = [this](
                std::set<int64_t>& nodes,
                int attr1,
                const std::string& key1,
                int attr2,
                const std::string& key2)
            {
                aggregator_->get_nodes2(nodes, attr1, key1, attr2, key2);
            };

            obj_get_f_ = [this](
                std::vector<std::string>& result,
                int64_t obj_id)
            {
                aggregator_->obj_get(result, obj_id);
            };

            assoc_range_f_ = [this](
                std::vector<ThriftAssoc>& _return,
                int64_t src,
                int64_t atype,
                int32_t off,
                int32_t len)
            {
                COND_LOG_E("about to call aggregator's assoc_range()\n");
                aggregator_->assoc_range(_return, src, atype, off, len);
            };

            assoc_get_f_ = [this](
                std::vector<ThriftAssoc>& _return,
                int64_t src,
                int64_t atype,
                const std::set<int64_t>& dst_id_set,
                int64_t t_low,
                int64_t t_high)
            {
                aggregator_->assoc_get(_return,
                    src, atype, dst_id_set, t_low, t_high);
            };

            assoc_count_f_ = [this](int64_t src, int64_t atype) {
                return aggregator_->assoc_count(src, atype);
            };

            assoc_time_range_f_ = [this](
                std::vector<ThriftAssoc>& _return,
                int64_t src,
                int64_t atype,
                int64_t t_low,
                int64_t t_high,
                int32_t len)
            {
                aggregator_->assoc_time_range(
                    _return, src, atype, t_low, t_high, len);
            };

        } else {
            // TODO: too lazy to add assignments for TAO functions in this case

            // not sharded, so call graph
            get_neighbors_f_ = [this](std::vector<int64_t>& nhbrs, int64_t id) {
                graph_->get_neighbors(nhbrs, id);
            };

            get_neighbors_atype_f_ = [this](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int64_t atype)
            {
                graph_->get_neighbors(nhbrs, id, atype);
            };

            get_neighbors_attr_f_ = [this](
                std::vector<int64_t>& nhbrs,
                int64_t id,
                int attr,
                const std::string& key)
            {
                graph_->get_neighbors(nhbrs, id, attr, key);
            };

            get_nodes_f_ = [this](
                std::set<int64_t>& nodes,
                int attr,
                const std::string& key)
            {
                graph_->get_nodes(nodes, attr, key);
            };

            get_nodes2_f_ = [this](
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

    void init_sharded_benchmark(const std::string& master_hostname) {
        try {
            LOG_E("Connecting to server '%s'...\n", master_hostname.c_str());
            shared_ptr<TSocket> socket(
                new TSocket(master_hostname, QUERY_HANDLER_PORT));
            transport_ = shared_ptr<TTransport>(new TBufferedTransport(socket));
            shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport_));
            aggregator_ = shared_ptr<GraphQueryAggregatorServiceClient>(
                new GraphQueryAggregatorServiceClient(protocol));
            transport_->open();
            LOG_E("Connected to aggregator!\n");

            int ret = aggregator_->init();
            LOG_E("Aggregator has init()'d cluster, return code = %d\n", ret);
        } catch (std::exception& e) {
            LOG_E("Exception in benchmark client: %s\n", e.what());
        }
    }

    ~GraphBenchmark() {
//        if (aggregator_ != nullptr) {
//            aggregator_->shutdown();
//        }
        if (transport_ != nullptr && transport_->isOpen()) {
            transport_->close();
        }
    }

    void cleanup() {
//        if (aggregator_ != nullptr) {
//            aggregator_->shutdown();
//            aggregator_ = nullptr;
//        }
        if (transport_ != nullptr && transport_->isOpen()) {
            transport_->close();
            transport_ = nullptr;
        }
    }

    void read_test_queries(const std::string& warmup_assoc_range_file,
            const std::string& assoc_range_file,
            const std::string& warmup_assoc_count_file,
            const std::string& assoc_count_file,
            const std::string& warmup_obj_get_file,
            const std::string& obj_get_file,
            const std::string& warmup_assoc_get_file,
            const std::string& assoc_get_file,
            const std::string& warmup_assoc_time_range_file,
            const std::string& assoc_time_range_file) {

    	// assoc_range
		read_assoc_range_queries(warmup_assoc_range_file, assoc_range_file);
		// assoc_count
		read_neighbor_atype_queries(warmup_assoc_count_file, assoc_count_file,
			warmup_assoc_count_nodes, assoc_count_nodes,
			warmup_assoc_count_atypes, assoc_count_atypes);
		// obj_get
		read_neighbor_queries(warmup_obj_get_file, obj_get_file,
			warmup_obj_get_nodes, obj_get_nodes);
		// assoc_get
		read_assoc_get_queries(warmup_assoc_get_file, assoc_get_file);
		// assoc_time_range
		read_assoc_time_range_queries(
			warmup_assoc_time_range_file, assoc_time_range_file);
    }

    void test_tao_query(int query_type, int query_idx) {
        shared_ptr<TSocket> socket(
            new TSocket("localhost", QUERY_HANDLER_PORT));
        shared_ptr<TTransport> transport(
            new TBufferedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        shared_ptr<GraphQueryAggregatorServiceClient> client(
            new GraphQueryAggregatorServiceClient(protocol));
        transport->open();
        client->init();

        std::vector<ThriftAssoc> result;
        std::vector<std::string> attrs;

        shared_ptr<benchmark_thread_data_t> thread_data(
            new benchmark_thread_data_t);
        try {
        switch (query_type) {
            case 0:
                LOG_E("assoc range, query idx %d (%d %d %d %d)\n",
                    query_idx,
                    warmup_assoc_range_nodes.size(),
                    warmup_assoc_range_atypes.size(),
                    warmup_assoc_range_offs.size(),
                    warmup_assoc_range_lens.size());
                client->assoc_range(result,
                    this->warmup_assoc_range_nodes.at(query_idx),
                    this->warmup_assoc_range_atypes.at(query_idx),
                    this->warmup_assoc_range_offs.at(query_idx),
                    this->warmup_assoc_range_lens.at(query_idx));
                break;
            case 1:
                LOG_E("obj_get, query idx %d (%d)\n",
                    query_idx, warmup_obj_get_nodes.size());
                client->obj_get(attrs,
                    this->warmup_obj_get_nodes.at(query_idx));
                break;
            case 2:
                LOG_E("assoc_get, query idx %d (%d %d %d %d %d)\n",
                    query_idx,
                    this->warmup_assoc_get_nodes.size(),
                    this->warmup_assoc_get_lows.size(),
                    this->warmup_assoc_get_dst_id_sets.size(),
                    this->warmup_assoc_get_lows.size(),
                    this->warmup_assoc_get_highs.size());

                client->assoc_get(result,
                    this->warmup_assoc_get_nodes.at(query_idx),
                    this->warmup_assoc_get_atypes.at(query_idx),
                    this->warmup_assoc_get_dst_id_sets.at(query_idx),
                    this->warmup_assoc_get_lows.at(query_idx),
                    this->warmup_assoc_get_highs.at(query_idx));
                break;
            case 3:
                LOG_E("assoc_count, query idx %d (%d %d)\n",
                    query_idx,
                     this->warmup_assoc_count_nodes.size(),
                     this->warmup_assoc_count_atypes.size());
                client->assoc_count(
                    this->warmup_assoc_count_nodes.at(query_idx),
                    this->warmup_assoc_count_atypes.at(query_idx));
                break;
            case 4:
                LOG_E("assoc_time_range, query idx %d (%d %d %d %d %d)\n",
                    query_idx,
                    this->warmup_assoc_time_range_nodes.size(),
                    this->warmup_assoc_time_range_atypes.size(),
                    this->warmup_assoc_time_range_lows.size(),
                    this->warmup_assoc_time_range_highs.size(),
                    this->warmup_assoc_time_range_limits.size());

                client->assoc_time_range(result,
                    this->warmup_assoc_time_range_nodes.at(query_idx),
                    this->warmup_assoc_time_range_atypes.at(query_idx),
                    this->warmup_assoc_time_range_lows.at(query_idx),
                    this->warmup_assoc_time_range_highs.at(query_idx),
                    this->warmup_assoc_time_range_limits.at(query_idx));
                break;
            default:
                assert(false);
        }
        } catch (std::exception& e) {
          LOG_E("Query failed: %s\n", e.what());
          return;
        }
        transport->close();
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
        read_neighbor_queries(warmup_query_file, query_file,
            warmup_neighbor_indices, neighbor_indices);
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        // Warmup
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for (uint64_t i = 0; i < WARMUP_N; ++i) {
            get_neighbors_f_(result, mod_get(warmup_neighbor_indices, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; ++i) {
            t0 = get_timestamp();
            get_neighbors_f_(result, mod_get(neighbor_indices, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "node id: " << mod_get(neighbor_indices, i)
                << "\n";
            std::sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it) {
                query_res_stream << *it << " ";
            }
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_neighbor_atype_throughput(
        const int num_threads,
        const std::string& master_hostname,
        std::string warmup_query_file,
        std::string query_file)
    {
        read_neighbor_atype_queries(warmup_query_file, query_file,
            warmup_nhbrAtype_indices, nhbrAtype_indices,
            warmup_atypes, atypes);
        bench_throughput(num_threads, master_hostname, BenchType::NHBR_ATYPE);
    }

    void benchmark_edge_attrs_throughput(
        const int num_threads,
        const std::string& master_hostname,
        std::string warmup_query_file,
        std::string query_file)
    {
        read_neighbor_atype_queries(warmup_query_file, query_file,
            warmup_nhbrAtype_indices, nhbrAtype_indices,
            warmup_atypes, atypes);
        bench_throughput(num_threads, master_hostname, BenchType::EDGE_ATTRS);
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
        read_neighbor_atype_queries(warmup_query_file, query_file,
            warmup_nhbrAtype_indices, nhbrAtype_indices,
            warmup_atypes, atypes);
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        // Warmup
        LOG_E("Warming up for %" PRIu64 " queries...\n", WARMUP_N);
        std::vector<int64_t> result;
        for (uint64_t i = 0; i < WARMUP_N; ++i) {
            get_neighbors_atype_f_(
                result,
                mod_get(warmup_nhbrAtype_indices, i),
                mod_get(warmup_atypes, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; ++i) {
            t0 = get_timestamp();
            get_neighbors_atype_f_(
                result,
                mod_get(nhbrAtype_indices, i),
                mod_get(atypes, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness validation
            query_res_stream << "node id: " << mod_get(nhbrAtype_indices, i)
                << "\n";
            query_res_stream << "atype:  " << mod_get(atypes, i) << "\n";
            std::sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it) {
                query_res_stream << *it << " ";
            }
            query_res_stream << "\n";
#endif
        }
        LOG_E("Measure complete.\n");
    }

    std::pair<double, double> benchmark_neighbor_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_size = warmup_neighbor_indices.size();
        size_t measure_size = neighbor_indices.size();
        COND_LOG_E("warmup size %lld, query size %lld\n", warmup_size, measure_size);

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_dis(0, warmup_size - 1);
        std::uniform_int_distribution<int> measure_dis(0, measure_size - 1);

        try {
            std::vector<int64_t> result;

            // Warmup phase
            int64_t i = 0;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                COND_LOG_E("warmup query %d\n", i);
                thread_data->client->get_neighbors(
                    result,
                    mod_get(warmup_neighbor_indices, warmup_dis(gen)));
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
                COND_LOG_E("measure query %d\n", i);
                thread_data->client->get_neighbors(
                    result, mod_get(neighbor_indices, measure_dis(gen)));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_get_nhbrs.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                thread_data->client->get_neighbors(
                    result, mod_get(neighbor_indices, i));
                ++i;
            }

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_neighbor_atype_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_size = warmup_nhbrAtype_indices.size();
        size_t measure_size = nhbrAtype_indices.size();

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_dis(0, warmup_size - 1);
        std::uniform_int_distribution<int> measure_dis(0, measure_size - 1);

        try {
            std::vector<int64_t> result;

            // Warmup phase
            int64_t i = 0;
            int query_idx;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                query_idx = warmup_dis(gen);
                thread_data->client->get_neighbors_atype(
                    result,
                    mod_get(warmup_nhbrAtype_indices, query_idx),
                    mod_get(warmup_atypes, query_idx));
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
                query_idx = measure_dis(gen);
                thread_data->client->get_neighbors_atype(
                    result,
                    mod_get(nhbrAtype_indices, query_idx),
                    mod_get(atypes, query_idx));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_get_nhbrsAtype.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                thread_data->client->get_neighbors_atype(
                    result,
                    mod_get(nhbrAtype_indices, query_idx),
                    mod_get(atypes, query_idx));
                ++query_idx;
            }

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_edge_attrs_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_size = warmup_nhbrAtype_indices.size();
        size_t measure_size = nhbrAtype_indices.size();

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_dis(0, warmup_size - 1);
        std::uniform_int_distribution<int> measure_dis(0, measure_size - 1);

        try {
            std::vector<std::string> result;

            // Warmup phase
            int64_t i = 0;
            int query_idx;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                query_idx = warmup_dis(gen);
                thread_data->client->get_edge_attrs(
                    result,
                    mod_get(warmup_nhbrAtype_indices, query_idx),
                    mod_get(warmup_atypes, query_idx));
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
                query_idx = measure_dis(gen);
                thread_data->client->get_edge_attrs(
                    result,
                    mod_get(nhbrAtype_indices, query_idx),
                    mod_get(atypes, query_idx));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_getEdgeAttrs.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                thread_data->client->get_edge_attrs(
                    result,
                    mod_get(nhbrAtype_indices, query_idx),
                    mod_get(atypes, query_idx));
                ++query_idx;
            }

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_neighbor_node_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_size = warmup_nhbrNode_indices.size();
        size_t measure_size = nhbrNode_indices.size();

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_dis(0, warmup_size - 1);
        std::uniform_int_distribution<int> measure_dis(0, measure_size - 1);

        try {
            std::vector<int64_t> result;

            // Warmup phase
            int64_t i = 0;
            int query_idx;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                query_idx = warmup_dis(gen);
                thread_data->client->get_neighbors_attr(
                    result,
                    mod_get(warmup_nhbrNode_indices, query_idx),
                    mod_get(warmup_nhbrNode_attr_ids, query_idx),
                    mod_get(warmup_nhbrNode_attrs, query_idx));
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
                query_idx = measure_dis(gen);
                thread_data->client->get_neighbors_attr(
                    result,
                    mod_get(nhbrNode_indices, query_idx),
                    mod_get(nhbrNode_attr_ids, query_idx),
                    mod_get(nhbrNode_attrs, query_idx));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_get_nhbrsNode.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                thread_data->client->get_neighbors_attr(
                    result,
                    mod_get(nhbrNode_indices, query_idx),
                    mod_get(nhbrNode_attr_ids, query_idx),
                    mod_get(nhbrNode_attrs, query_idx));
                ++query_idx;
            }


        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_node_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_size = warmup_node_attributes.size();
        size_t measure_size = node_attributes.size();

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_dis(0, warmup_size - 1);
        std::uniform_int_distribution<int> measure_dis(0, measure_size - 1);

        try {
            std::set<int64_t> result;

            // Warmup phase
            int64_t i = 0;
            int query_idx;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                query_idx = warmup_dis(gen);
                thread_data->client->get_nodes(
                    result, mod_get(warmup_node_attributes, query_idx),
                    mod_get(warmup_node_queries, query_idx));
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
                query_idx = measure_dis(gen);
                thread_data->client->get_nodes(
                    result, mod_get(node_attributes, query_idx),
                    mod_get(node_queries, query_idx));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_get_nodes.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                thread_data->client->get_nodes(
                    result, mod_get(node_attributes, query_idx),
                    mod_get(node_queries, query_idx));
                ++query_idx;
            }

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_node_node_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_size = warmup_node_attributes.size();
        size_t measure_size = node_attributes.size();

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_dis(0, warmup_size - 1);
        std::uniform_int_distribution<int> measure_dis(0, measure_size - 1);

        try {
            std::set<int64_t> result;

            // Warmup phase
            int64_t i = 0;
            int query_idx;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                query_idx = warmup_dis(gen);
                thread_data->client->get_nodes2(
                    result,
                    mod_get(warmup_node_attributes, query_idx),
                    mod_get(warmup_node_queries, query_idx),
                    mod_get(warmup_node_attributes2, query_idx),
                    mod_get(warmup_node_queries2, query_idx));
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
                query_idx = measure_dis(gen);
                thread_data->client->get_nodes2(
                    result,
                    mod_get(node_attributes, query_idx),
                    mod_get(node_queries, query_idx),
                    mod_get(node_attributes2, query_idx),
                    mod_get(node_queries2, query_idx));
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_get_nodes2.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                thread_data->client->get_nodes2(
                    result,
                    mod_get(node_attributes, query_idx),
                    mod_get(node_queries, query_idx),
                    mod_get(node_attributes2, query_idx),
                    mod_get(node_queries2, query_idx));
                ++query_idx;
            }

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_mix_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        size_t warmup_nhbr_size = warmup_neighbor_indices.size();
        size_t warmup_nhbr_node_size = warmup_nhbrNode_indices.size();
        size_t warmup_node_size = warmup_node_attributes.size();
        size_t warmup_nhbr_atype_size = warmup_nhbrAtype_indices.size();
        size_t nhbr_size = neighbor_indices.size();
        size_t nhbr_node_size = nhbrNode_indices.size();
        size_t node_size = node_attributes.size();
        size_t nhbr_atype_size = nhbrAtype_indices.size();

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> warmup_nhbr_dis(
            0, warmup_nhbr_size - 1);
        std::uniform_int_distribution<int> warmup_nhbr_node_dis(
            0, warmup_nhbr_node_size);
        std::uniform_int_distribution<int> warmup_node_dis(
            0, warmup_node_size);
        std::uniform_int_distribution<int> warmup_nhbr_atype_dis(
            0, warmup_nhbr_atype_size);
        std::uniform_int_distribution<int> nhbr_dis(0, nhbr_size - 1);
        std::uniform_int_distribution<int> nhbr_node_dis(0, nhbr_node_size);
        std::uniform_int_distribution<int> node_dis(0, node_size);
        std::uniform_int_distribution<int> nhbr_atype_dis(0, nhbr_atype_size);
        std::uniform_int_distribution<int> query_dis(0, 4);

        try {
            std::vector<int64_t> result;
            std::vector<std::string> attrs;
            std::set<int64_t> result_set;

            // Warmup phase
            int64_t i = 0;
            int query_idx, rand_query;
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                rand_query = query_dis(gen);
                switch (rand_query) {
                case 0:
                    thread_data->client->get_neighbors(result,
                        mod_get(warmup_neighbor_indices, warmup_nhbr_dis(gen)));
                    break;
                case 1:
                    query_idx = warmup_nhbr_node_dis(gen);
                    thread_data->client->get_neighbors_attr(result,
                        mod_get(warmup_nhbrNode_indices, query_idx),
                        mod_get(warmup_nhbrNode_attr_ids, query_idx),
                        mod_get(warmup_nhbrNode_attrs, query_idx));
                    break;
                case 2:
                    query_idx = warmup_nhbr_atype_dis(gen);
                    thread_data->client->get_edge_attrs(attrs,
                        mod_get(warmup_nhbrAtype_indices, query_idx),
                        mod_get(warmup_atypes, query_idx));
                    break;
                case 3:
                    query_idx = warmup_nhbr_atype_dis(gen);
                    thread_data->client->get_neighbors_atype(result,
                        mod_get(warmup_nhbrAtype_indices, query_idx),
                        mod_get(warmup_atypes, query_idx));
                    break;
                case 4:
                    query_idx = warmup_node_dis(gen);
                    thread_data->client->get_nodes2(result_set,
                        mod_get(warmup_node_attributes, query_idx),
                        mod_get(warmup_node_queries, query_idx),
                        mod_get(warmup_node_attributes2, query_idx),
                        mod_get(warmup_node_queries2, query_idx));
                    break;
                default:
                    assert(false);
                }
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
#ifndef RUN_MIX_THPUT_BODY
#define RUN_MIX_THPUT_BODY
                rand_query = query_dis(gen); \
                switch (rand_query) { \
                case 0: \
                    query_idx = nhbr_dis(gen); \
                    thread_data->client->get_neighbors(result, \
                        mod_get(neighbor_indices, query_idx)); \
                    break; \
                case 1:                                     \
                    query_idx = nhbr_node_dis(gen); \
                    thread_data->client->get_neighbors_attr(result, \
                        mod_get(nhbrNode_indices, query_idx), \
                        mod_get(nhbrNode_attr_ids, query_idx), \
                        mod_get(nhbrNode_attrs, query_idx)); \
                    break; \
                case 2:                                             \
                    query_idx = nhbr_atype_dis(gen); \
                    thread_data->client->get_edge_attrs(attrs, \
                        mod_get(nhbrAtype_indices, query_idx), \
                        mod_get(atypes, query_idx)); \
                    break; \
                case 3:                               \
                    query_idx = nhbr_atype_dis(gen); \
                    thread_data->client->get_neighbors_atype(result, \
                        mod_get(nhbrAtype_indices, query_idx), \
                        mod_get(atypes, query_idx)); \
                    break; \
                case 4:                                  \
                    query_idx = node_dis(gen); \
                    thread_data->client->get_nodes2(result_set, \
                        mod_get(node_attributes, query_idx), \
                        mod_get(node_queries, query_idx), \
                        mod_get(node_attributes2, query_idx), \
                        mod_get(node_queries2, query_idx)); \
                    break; \
                default: \
                    assert(false); \
                }
#endif
                RUN_MIX_THPUT_BODY
                edges += result.size();
                ++i;
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_mix.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
                RUN_MIX_THPUT_BODY
                ++query_idx;
            }

        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    void benchmark_neighbor_throughput(
        int num_threads,
        const std::string& master_hostname,
        const std::string& warmup_neighbor_query_file,
        const std::string& neighbor_query_file)
    {
        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file,
            warmup_neighbor_indices, neighbor_indices);
        bench_throughput(num_threads, master_hostname, BenchType::NHBR);
    }


    typedef struct {
        int64_t src, atype;
        int32_t off, len;
    } AssocRangeQuery;

    typedef struct {
        int64_t src, atype, tLow, tHigh;
        std::set<int64_t> dstIdSet;
    } AssocGetQuery;

    typedef struct {
        int64_t src, atype, tLow, tHigh;
        int32_t limit;
    } AssocTimeRangeQuery;

    typedef struct {
        int64_t src, atype;
    } AssocCountQuery;

    std::pair<double, double> benchmark_tao_mix_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        int query, query_idx;

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist_query(0, 4);

        std::uniform_int_distribution<int> warmup_assoc_range_size(
			0, warmup_assoc_range_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_obj_get_size(
			0, warmup_obj_get_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_assoc_count_size(
			0, warmup_assoc_count_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_assoc_time_range_size(
			0, warmup_assoc_time_range_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_assoc_get_size(
			0, warmup_assoc_get_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_range_size(
			0, assoc_range_nodes.size() - 1);
        std::uniform_int_distribution<int> obj_get_size(
			0, obj_get_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_count_size(
			0, assoc_count_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_time_range_size(
			0, assoc_time_range_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_get_size(
			0, assoc_get_nodes.size() - 1);

        COND_LOG_E("warmup assoc range: %lld, query %lld\n",
            warmup_assoc_range_nodes.size(), assoc_range_nodes.size());

        std::uniform_real_distribution<double> query_dis(0, 1);

        // non-batched
        std::vector<ThriftAssoc> result;
        std::vector<std::string> attrs;

#ifdef BATCH_QUERY
        std::vector<std::vector<ThriftAssoc>> assoc_range_results;
        std::vector<std::vector<ThriftAssoc>> assoc_get_results;
        std::vector<std::vector<ThriftAssoc>> assoc_time_range_results;
        std::vector<int64_t> assoc_count_results;
        std::vector<std::vector<std::string>> obj_get_results;

        // Batched queries
        // assoc_range()
        std::vector<int64_t> batched_assoc_range_nodes;
        std::vector<int64_t> batched_assoc_range_atypes;
        std::vector<int32_t> batched_assoc_range_offs;
        std::vector<int32_t> batched_assoc_range_lens;

        // batched_assoc_count()
        std::vector<int64_t> batched_assoc_count_nodes;
        std::vector<int64_t> batched_assoc_count_atypes;

        // obj_get
        std::vector<int64_t> batched_obj_get_nodes;

        // batched_assoc_get()
        std::vector<int64_t> batched_assoc_get_nodes;
        std::vector<int64_t>  batched_assoc_get_atypes;
        std::vector<std::set<int64_t>> batched_assoc_get_dst_id_sets;
        std::vector<int64_t> batched_assoc_get_highs;
        std::vector<int64_t> batched_assoc_get_lows;

        // batched_assoc_time_range()
        std::vector<int64_t> batched_assoc_time_range_nodes;
        std::vector<int64_t> batched_assoc_time_range_atypes;
        std::vector<int64_t> batched_assoc_time_range_highs;
        std::vector<int64_t> batched_assoc_time_range_lows;
        std::vector<int32_t> batched_assoc_time_range_limits;

        auto clear_batches = [&] {
            batched_assoc_range_nodes.clear();
            batched_assoc_range_atypes.clear();
            batched_assoc_range_offs.clear();
            batched_assoc_range_lens.clear();

            batched_assoc_count_nodes.clear();
            batched_assoc_count_atypes.clear();

            batched_obj_get_nodes.clear();

            batched_assoc_get_nodes.clear();
            batched_assoc_get_atypes.clear();
            batched_assoc_get_dst_id_sets.clear();
            batched_assoc_get_highs.clear();
            batched_assoc_get_lows.clear();

            batched_assoc_time_range_nodes.clear();
            batched_assoc_time_range_atypes.clear();
            batched_assoc_time_range_highs.clear();
            batched_assoc_time_range_lows.clear();
            batched_assoc_time_range_limits.clear();
        };

        auto send_batches = [&] {
            if (!batched_assoc_range_nodes.empty()) {
                thread_data->client->send_assoc_range_batched(
                    batched_assoc_range_nodes, batched_assoc_range_atypes,
                    batched_assoc_range_offs, batched_assoc_range_lens);
            }

            if (!batched_assoc_time_range_nodes.empty()) {
                thread_data->client->send_assoc_time_range_batched(
                    batched_assoc_time_range_nodes,
                    batched_assoc_time_range_atypes,
                    batched_assoc_time_range_lows,
                    batched_assoc_time_range_highs,
                    batched_assoc_time_range_limits);
            }

            if (!batched_assoc_get_nodes.empty()) {
                thread_data->client->send_assoc_get_batched(
                    batched_assoc_get_nodes,
                    batched_assoc_get_atypes,
                    batched_assoc_get_dst_id_sets,
                    batched_assoc_get_lows,
                    batched_assoc_get_highs);
            }

            if (!batched_assoc_count_nodes.empty()) {
                thread_data->client->send_assoc_count_batched(
                    batched_assoc_count_nodes, batched_assoc_count_atypes);
            }

            if (!batched_obj_get_nodes.empty()) {
                thread_data->client->send_obj_get_batched(
                    batched_obj_get_nodes);
            }
        };

        auto receive_batches = [&] {
            // Receive -- must be FIFO order
            if (!batched_assoc_range_nodes.empty()) {
                thread_data->client->recv_assoc_range_batched(
                    assoc_range_results);
            }

            if (!batched_assoc_time_range_nodes.empty()) {
                thread_data->client->recv_assoc_time_range_batched(
                    assoc_time_range_results);
            }

            if (!batched_assoc_get_nodes.empty()) {
                thread_data->client->recv_assoc_get_batched(
                    assoc_get_results);
            }

            if (!batched_assoc_count_nodes.empty()) {
                thread_data->client->recv_assoc_count_batched(
                    assoc_count_results);
            }

            if (!batched_obj_get_nodes.empty()) {
                thread_data->client->recv_obj_get_batched(
                    obj_get_results);
            }
        };
#endif

        int64_t i = 0, batches = 0;
        try {
            // Warmup phase
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                COND_LOG_E("warmup query %d\n", i);
#ifndef BATCH_QUERY
                query = choose_query(query_dis(gen));
                try {
                switch (query) {
                case 0:
                    query_idx = warmup_assoc_range_size(gen);
                    COND_LOG_E("assoc range, query idx %d (%d %d %d %d)\n",
                        query_idx,
                        warmup_assoc_range_nodes.size(),
                        warmup_assoc_range_atypes.size(),
                        warmup_assoc_range_offs.size(),
                        warmup_assoc_range_lens.size());
                    thread_data->client->assoc_range(result,
                        this->warmup_assoc_range_nodes.at(query_idx),
                        this->warmup_assoc_range_atypes.at(query_idx),
                        this->warmup_assoc_range_offs.at(query_idx),
                        this->warmup_assoc_range_lens.at(query_idx));
                    break;
                case 1:
                    query_idx = warmup_obj_get_size(gen);
                    COND_LOG_E("obj_get, query idx %d (%d)\n",
                        query_idx, warmup_obj_get_nodes.size());
                    thread_data->client->obj_get(attrs,
                        this->warmup_obj_get_nodes.at(query_idx));
                    break;
                case 2:
                    query_idx = warmup_assoc_get_size(gen);
                    COND_LOG_E("assoc_get, query idx %d (%d %d %d %d %d)\n",
                        query_idx,
                        this->warmup_assoc_get_nodes.size(),
                        this->warmup_assoc_get_lows.size(),
                        this->warmup_assoc_get_dst_id_sets.size(),
                        this->warmup_assoc_get_lows.size(),
                        this->warmup_assoc_get_highs.size());

                    thread_data->client->assoc_get(result,
                        this->warmup_assoc_get_nodes.at(query_idx),
                        this->warmup_assoc_get_atypes.at(query_idx),
                        this->warmup_assoc_get_dst_id_sets.at(query_idx),
                        this->warmup_assoc_get_lows.at(query_idx),
                        this->warmup_assoc_get_highs.at(query_idx));
                    break;
                case 3:
                    query_idx = warmup_assoc_count_size(gen);
                    COND_LOG_E("assoc_count, query idx %d (%d %d)\n",
                        query_idx,
                         this->warmup_assoc_count_nodes.size(),
                         this->warmup_assoc_count_atypes.size());
                    thread_data->client->assoc_count(
                        this->warmup_assoc_count_nodes.at(query_idx),
                        this->warmup_assoc_count_atypes.at(query_idx));
                    break;
                case 4:
                    query_idx = warmup_assoc_time_range_size(gen);
                    COND_LOG_E("assoc_time_range, query idx %d (%d %d %d %d %d)\n",
                        query_idx,
                        this->warmup_assoc_time_range_nodes.size(),
                        this->warmup_assoc_time_range_atypes.size(),
                        this->warmup_assoc_time_range_lows.size(),
                        this->warmup_assoc_time_range_highs.size(),
                        this->warmup_assoc_time_range_limits.size());

                    thread_data->client->assoc_time_range(result,
                        this->warmup_assoc_time_range_nodes.at(query_idx),
                        this->warmup_assoc_time_range_atypes.at(query_idx),
                        this->warmup_assoc_time_range_lows.at(query_idx),
                        this->warmup_assoc_time_range_highs.at(query_idx),
                        this->warmup_assoc_time_range_limits.at(query_idx));
                    break;
                default:
                    assert(false);
                }
                } catch (std::exception& e) {
                  fprintf(stderr, "Query failed: type = %d, idx = %d err = %s\n", query, query_idx, e.what());
                  thread_data->client.reset();
                  thread_data->transport.reset();
                  shared_ptr<TSocket> socket(
                      new TSocket(thread_data->master_hostname, QUERY_HANDLER_PORT));
                  shared_ptr<TTransport> transport(
                      new TBufferedTransport(socket));
                  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                  shared_ptr<GraphQueryAggregatorServiceClient> client(
                      new GraphQueryAggregatorServiceClient(protocol));
                  transport->open();
                  client->init();

                  thread_data->client = client;
                  thread_data->transport = transport;
                }
#else
                clear_batches();
                for (int j = 0; j < query_batch_size; ++j) {
                    query = choose_query(query_dis(gen));
                    switch (query) {
                    case 0:
                        query_idx = warmup_assoc_range_size(gen);
                        batched_assoc_range_nodes.push_back(
                            this->warmup_assoc_range_nodes.at(query_idx));
                        batched_assoc_range_atypes.push_back(
                            this->warmup_assoc_range_atypes.at(query_idx));
                        batched_assoc_range_offs.push_back(
                            this->warmup_assoc_range_offs.at(query_idx));
                        batched_assoc_range_lens.push_back(
                            this->warmup_assoc_range_lens.at(query_idx));
                        break;
                    case 1:
                        query_idx = warmup_obj_get_size(gen);
                        batched_obj_get_nodes.push_back(
                            this->warmup_obj_get_nodes.at(query_idx));
                        break;
                    case 2:
                        query_idx = warmup_assoc_get_size(gen);
                        batched_assoc_get_nodes.push_back(
                            this->warmup_assoc_get_nodes.at(query_idx));
                        batched_assoc_get_atypes.push_back(
                            this->warmup_assoc_get_atypes.at(query_idx));
                        batched_assoc_get_lows.push_back(
                            this->warmup_assoc_get_lows.at(query_idx));
                        batched_assoc_get_highs.push_back(
                            this->warmup_assoc_get_highs.at(query_idx));
                        batched_assoc_get_dst_id_sets.emplace_back(
                            std::move(this->warmup_assoc_get_dst_id_sets.at(query_idx)));
                        break;
                    case 3:
                        query_idx = warmup_assoc_count_size(gen);
                        batched_assoc_count_nodes.push_back(
                            this->warmup_assoc_count_nodes.at(query_idx));
                        batched_assoc_count_atypes.push_back(
                            this->warmup_assoc_count_atypes.at(query_idx));
                        break;
                    case 4:
                        query_idx = warmup_assoc_time_range_size(gen);
                        batched_assoc_time_range_nodes.push_back(
                            this->warmup_assoc_time_range_nodes.at(query_idx));
                        batched_assoc_time_range_atypes.push_back(
                            this->warmup_assoc_time_range_atypes.at(query_idx));
                        batched_assoc_time_range_lows.push_back(
                            this->warmup_assoc_time_range_lows.at(query_idx));
                        batched_assoc_time_range_highs.push_back(
                            this->warmup_assoc_time_range_highs.at(query_idx));
                        batched_assoc_time_range_limits.push_back(
                            this->warmup_assoc_time_range_limits.at(query_idx));
                        break;
                    default:
                        assert(false);
                    }
                }
                send_batches();
                receive_batches();
#endif // batch query
                ++i;
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries/batches\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
#ifndef BATCH_QUERY
#define RUN_TAO_MIX_THPUT_BODY
                query = choose_query(query_dis(gen)); \
                switch (query) { \
                case 0: \
                  query_idx = assoc_range_size(gen); \
                  thread_data->client->assoc_range(result, \
                      this->assoc_range_nodes.at(query_idx), \
                      this->assoc_range_atypes.at(query_idx), \
                      this->assoc_range_offs.at(query_idx), \
                      this->assoc_range_lens.at(query_idx)); \
                  break; \
                case 1: \
                  query_idx = obj_get_size(gen); \
                  thread_data->client->obj_get(attrs, \
                      this->obj_get_nodes.at(query_idx)); \
                  break; \
                case 2: \
                  query_idx = assoc_get_size(gen); \
                  thread_data->client->assoc_get(result, \
                      this->assoc_get_nodes.at(query_idx), \
                      this->assoc_get_atypes.at(query_idx), \
                      this->assoc_get_dst_id_sets.at(query_idx), \
                      this->assoc_get_lows.at(query_idx), \
                      this->assoc_get_highs.at(query_idx)); \
                  break; \
                case 3: \
                  query_idx = assoc_count_size(gen); \
                  thread_data->client->assoc_count( \
                      this->assoc_count_nodes.at(query_idx), \
                      this->assoc_count_atypes.at(query_idx)); \
                  break; \
                case 4: \
                  query_idx = assoc_time_range_size(gen); \
                  thread_data->client->assoc_time_range(result, \
                      this->assoc_time_range_nodes.at(query_idx), \
                      this->assoc_time_range_atypes.at(query_idx), \
                      this->assoc_time_range_lows.at(query_idx), \
                      this->assoc_time_range_highs.at(query_idx), \
                      this->assoc_time_range_limits.at(query_idx)); \
                  break; \
                default: \
                  assert(false); \
                } \
                edges += result.size(); \
                ++i;
#else // BATCH_QUERY
#define RUN_TAO_MIX_THPUT_BODY_BATCHED
               clear_batches(); \
               for (int j = 0; j < query_batch_size; ++j) { \
                    query = choose_query(query_dis(gen)); \
                    switch (query) { \
                    case 0: \
                        query_idx = assoc_range_size(gen); \
                        batched_assoc_range_nodes.push_back( \
                            this->assoc_range_nodes.at(query_idx)); \
                        batched_assoc_range_atypes.push_back( \
                            this->assoc_range_atypes.at(query_idx)); \
                        batched_assoc_range_offs.push_back( \
                            this->assoc_range_offs.at(query_idx)); \
                        batched_assoc_range_lens.push_back( \
                            this->assoc_range_lens.at(query_idx)); \
                        break; \
                    case 1: \
                        query_idx = obj_get_size(gen); \
                        batched_obj_get_nodes.push_back( \
                            this->obj_get_nodes.at(query_idx)); \
                        break; \
                    case 2: \
                        query_idx = assoc_get_size(gen); \
                        batched_assoc_get_nodes.push_back( \
                            this->assoc_get_nodes.at(query_idx)); \
                        batched_assoc_get_atypes.push_back( \
                            this->assoc_get_atypes.at(query_idx)); \
                        batched_assoc_get_lows.push_back( \
                            this->assoc_get_lows.at(query_idx)); \
                        batched_assoc_get_highs.push_back( \
                            this->assoc_get_highs.at(query_idx)); \
                        batched_assoc_get_dst_id_sets.emplace_back( \
                            std::move(this->assoc_get_dst_id_sets.at(query_idx))); \
                        break; \
                    case 3: \
                        query_idx = assoc_count_size(gen); \
                        batched_assoc_count_nodes.push_back( \
                            this->assoc_count_nodes.at(query_idx)); \
                        batched_assoc_count_atypes.push_back( \
                            this->assoc_count_atypes.at(query_idx)); \
                        break; \
                    case 4: \
                        query_idx = assoc_time_range_size(gen); \
                        batched_assoc_time_range_nodes.push_back( \
                            this->assoc_time_range_nodes.at(query_idx)); \
                        batched_assoc_time_range_atypes.push_back( \
                            this->assoc_time_range_atypes.at(query_idx)); \
                        batched_assoc_time_range_lows.push_back( \
                            this->assoc_time_range_lows.at(query_idx)); \
                        batched_assoc_time_range_highs.push_back( \
                            this->assoc_time_range_highs.at(query_idx)); \
                        batched_assoc_time_range_limits.push_back( \
                            this->assoc_time_range_limits.at(query_idx)); \
                        break; \
                    default: \
                        assert(false); \
                    } \
                } \
                send_batches(); \
                receive_batches(); \
                edges += assoc_range_results.size() + \
                    assoc_get_results.size() + \
                    assoc_time_range_results.size(); \
                ++batches;
                i += query_batch_size;
#endif // BATCH_QUERY

#ifndef BATCH_QUERY
                try {
                RUN_TAO_MIX_THPUT_BODY // actually run
                } catch (std::exception& e) {
                  fprintf(stderr, "Query failed: type = %d, idx = %d err = %s\n", query, query_idx, e.what());
                  thread_data->client.reset();
                  thread_data->transport.reset();
                  shared_ptr<TSocket> socket(
                      new TSocket(thread_data->master_hostname, QUERY_HANDLER_PORT));
                  shared_ptr<TTransport> transport(
                      new TBufferedTransport(socket));
                  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                  shared_ptr<GraphQueryAggregatorServiceClient> client(
                      new GraphQueryAggregatorServiceClient(protocol));
                  transport->open();
                  client->init();

                  thread_data->client = client;
                  thread_data->transport = transport;
                }
#else
                RUN_TAO_MIX_THPUT_BODY_BATCHED // run the batched version
#endif
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
#ifndef BATCH_QUERY
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);
#else
            COND_LOG_E(
                "Query done: served %" PRId64 " queries, %lld %d-batches\n",
                i, batches, query_batch_size);
#endif

            std::ofstream ofs("throughput_tao_mix.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
#ifndef BATCH_QUERY
              try {
                RUN_TAO_MIX_THPUT_BODY
              } catch (std::exception& e) {
                fprintf(stderr, "Query failed: type = %d, idx = %d err = %s\n", query, query_idx, e.what());
                thread_data->client.reset();
                thread_data->transport.reset();
                shared_ptr<TSocket> socket(
                    new TSocket(thread_data->master_hostname, QUERY_HANDLER_PORT));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                shared_ptr<GraphQueryAggregatorServiceClient> client(
                    new GraphQueryAggregatorServiceClient(protocol));
                transport->open();
                client->init();

                thread_data->client = client;
                thread_data->transport = transport;
              }
#else
                RUN_TAO_MIX_THPUT_BODY_BATCHED
#endif
            }
        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    std::pair<double, double> benchmark_tao_mix_with_updates_throughput_helper(
        shared_ptr<benchmark_thread_data_t> thread_data)
    {
        double query_thput = 0;
        double edges_thput = 0;
        COND_LOG_E("About to start querying on this thread...\n");

        int query, query_idx;

        thread_local std::random_device rd;
        thread_local std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist_query(0, 4);

        std::uniform_int_distribution<int> warmup_assoc_range_size(
			0, warmup_assoc_range_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_obj_get_size(
			0, warmup_obj_get_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_assoc_count_size(
			0, warmup_assoc_count_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_assoc_time_range_size(
			0, warmup_assoc_time_range_nodes.size() - 1);
        std::uniform_int_distribution<int> warmup_assoc_get_size(
			0, warmup_assoc_get_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_range_size(
			0, assoc_range_nodes.size() - 1);
        std::uniform_int_distribution<int> obj_get_size(
			0, obj_get_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_count_size(
			0, assoc_count_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_time_range_size(
			0, assoc_time_range_nodes.size() - 1);
        std::uniform_int_distribution<int> assoc_get_size(
			0, assoc_get_nodes.size() - 1);

        COND_LOG_E("warmup assoc range: %lld, query %lld\n",
            warmup_assoc_range_nodes.size(), assoc_range_nodes.size());

        std::uniform_real_distribution<double> query_dis(0, 1);

        std::uniform_int_distribution<int64_t> dist_node(0, NUM_NODES - 1);
        std::uniform_int_distribution<int64_t> dist_atype(0, NUM_ATYPES - 1);

        // non-batched
        std::vector<ThriftAssoc> result;
        std::vector<std::string> attrs;

        int64_t i = 0, batches = 0;
        int64_t src, atype, dst;
        int ret;

        try {
            // Warmup phase
            time_t start = get_timestamp();
            while (get_timestamp() - start < WARMUP_MICROSECS) {
                COND_LOG_E("warmup query %d\n", i);
                query = choose_query_with_updates(
                    query_dis(gen), query_dis(gen));
                try {
                switch (query) {
                case 0:
                    query_idx = warmup_assoc_range_size(gen);
                    COND_LOG_E("assoc range, query idx %d (%d %d %d %d)\n",
                        query_idx,
                        warmup_assoc_range_nodes.size(),
                        warmup_assoc_range_atypes.size(),
                        warmup_assoc_range_offs.size(),
                        warmup_assoc_range_lens.size());
                    thread_data->client->assoc_range(result,
                        this->warmup_assoc_range_nodes.at(query_idx),
                        this->warmup_assoc_range_atypes.at(query_idx),
                        this->warmup_assoc_range_offs.at(query_idx),
                        this->warmup_assoc_range_lens.at(query_idx));
                    break;
                case 1:
                    query_idx = warmup_obj_get_size(gen);
                    COND_LOG_E("obj_get, query idx %d\n", query_idx);
                    thread_data->client->obj_get(attrs,
                        this->warmup_obj_get_nodes.at(query_idx));
                    break;
                case 2:
                    query_idx = warmup_assoc_get_size(gen);
                    COND_LOG_E("assoc_get, query idx %d\n", query_idx);
                    thread_data->client->assoc_get(result,
                        this->warmup_assoc_get_nodes.at(query_idx),
                        this->warmup_assoc_get_atypes.at(query_idx),
                        this->warmup_assoc_get_dst_id_sets.at(query_idx),
                        this->warmup_assoc_get_lows.at(query_idx),
                        this->warmup_assoc_get_highs.at(query_idx));
                    break;
                case 3:
                    query_idx = warmup_assoc_count_size(gen);
                    COND_LOG_E("assoc_count, query idx %d\n", query_idx);
                    thread_data->client->assoc_count(
                        this->warmup_assoc_count_nodes.at(query_idx),
                        this->warmup_assoc_count_atypes.at(query_idx));
                    break;
                case 4:
                    query_idx = warmup_assoc_time_range_size(gen);
                    COND_LOG_E("assoc_time_range, query idx %d\n", query_idx);
                    thread_data->client->assoc_time_range(result,
                        this->warmup_assoc_time_range_nodes.at(query_idx),
                        this->warmup_assoc_time_range_atypes.at(query_idx),
                        this->warmup_assoc_time_range_lows.at(query_idx),
                        this->warmup_assoc_time_range_highs.at(query_idx),
                        this->warmup_assoc_time_range_limits.at(query_idx));
                    break;
                case 5:
                    src = dist_node(gen);
                    atype = dist_atype(gen);
                    dst = dist_node(gen);
                    COND_LOG_E("assoc_add(%lld,atype %d,%lld,...) ",
                        src, atype, dst);
                    ret = thread_data->client->assoc_add(
                        src,
                        atype,
                        dst,
                        MAX_TIME,
                        ATTR_FOR_NEW_EDGES);
                    COND_LOG_E("; ret = %d\n", ret);
                    break;
                default:
                    assert(false);
                }

                ++i;
                } catch (std::exception& e) {
                  fprintf(stderr, "Query failed: type = %d, idx = %d err = %s\n", query, query_idx, e.what());
                  thread_data->client.reset();
                  thread_data->transport.reset();
                  shared_ptr<TSocket> socket(
                      new TSocket(thread_data->master_hostname, QUERY_HANDLER_PORT));
                  shared_ptr<TTransport> transport(
                      new TBufferedTransport(socket));
                  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                  shared_ptr<GraphQueryAggregatorServiceClient> client(
                      new GraphQueryAggregatorServiceClient(protocol));
                  transport->open();
                  client->init();

                  thread_data->client = client;
                  thread_data->transport = transport;
                }
            }
            COND_LOG_E("Warmup done: served %" PRId64 " queries/batches\n", i);

            // Measure phase
            i = 0;
            int64_t edges = 0;
            start = get_timestamp();
            while (get_timestamp() - start < MEASURE_MICROSECS) {
#define RUN_TAO_MIX_WITH_UPDATES_THPUT_BODY
                query = choose_query_with_updates(query_dis(gen), query_dis(gen)); \
                switch (query) { \
                case 0: \
                  query_idx = assoc_range_size(gen); \
                  thread_data->client->assoc_range(result, \
                      this->assoc_range_nodes.at(query_idx), \
                      this->assoc_range_atypes.at(query_idx), \
                      this->assoc_range_offs.at(query_idx), \
                      this->assoc_range_lens.at(query_idx)); \
                  break; \
                case 1: \
                  query_idx = obj_get_size(gen); \
                  thread_data->client->obj_get(attrs, \
                      this->obj_get_nodes.at(query_idx)); \
                  break; \
                case 2: \
                  query_idx = assoc_get_size(gen); \
                  thread_data->client->assoc_get(result, \
                      this->assoc_get_nodes.at(query_idx), \
                      this->assoc_get_atypes.at(query_idx), \
                      this->assoc_get_dst_id_sets.at(query_idx), \
                      this->assoc_get_lows.at(query_idx), \
                      this->assoc_get_highs.at(query_idx)); \
                  break; \
                case 3: \
                  query_idx = assoc_count_size(gen); \
                  thread_data->client->assoc_count( \
                      this->assoc_count_nodes.at(query_idx), \
                      this->assoc_count_atypes.at(query_idx)); \
                  break; \
                case 4: \
                  query_idx = assoc_time_range_size(gen); \
                  thread_data->client->assoc_time_range(result, \
                      this->assoc_time_range_nodes.at(query_idx), \
                      this->assoc_time_range_atypes.at(query_idx), \
                      this->assoc_time_range_lows.at(query_idx), \
                      this->assoc_time_range_highs.at(query_idx), \
                      this->assoc_time_range_limits.at(query_idx)); \
                  break; \
                case 5: \
                    src = dist_node(gen); \
                    atype = dist_atype(gen); \
                    dst = dist_node(gen); \
                    thread_data->client->assoc_add(src, atype, dst, MAX_TIME, ATTR_FOR_NEW_EDGES); \
                    break; \
                default: \
                  assert(false); \
                } \
                edges += result.size(); \
                ++i;
                try {
                  RUN_TAO_MIX_WITH_UPDATES_THPUT_BODY // actually run
                } catch (std::exception& e) {
                  fprintf(stderr, "Query failed: type = %d, idx = %d err = %s\n", query, query_idx, e.what());
                  thread_data->client.reset();
                  thread_data->transport.reset();
                  shared_ptr<TSocket> socket(
                      new TSocket(thread_data->master_hostname, QUERY_HANDLER_PORT));
                  shared_ptr<TTransport> transport(
                      new TBufferedTransport(socket));
                  shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                  shared_ptr<GraphQueryAggregatorServiceClient> client(
                      new GraphQueryAggregatorServiceClient(protocol));
                  transport->open();
                  client->init();

                  thread_data->client = client;
                  thread_data->transport = transport;
                }
            }
            time_t end = get_timestamp();
            double total_secs = (end - start) * 1. / 1e6;
            query_thput = i * 1. / total_secs;
            edges_thput = edges * 1. / total_secs;
            COND_LOG_E("Query done: served %" PRId64 " queries\n", i);

            std::ofstream ofs("throughput_taoMixWithUpdates.txt",
                std::ofstream::out | std::ofstream::app);
            ofs << query_thput << " " << edges_thput << std::endl;
            ofs.close();

            // Cooldown
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_MICROSECS) {
              try {
                RUN_TAO_MIX_WITH_UPDATES_THPUT_BODY
              } catch(std::exception& e) {
                fprintf(stderr, "Query failed: type = %d, idx = %d err = %s\n", query, query_idx, e.what());
                thread_data->client.reset();
                thread_data->transport.reset();
                shared_ptr<TSocket> socket(
                    new TSocket(thread_data->master_hostname, QUERY_HANDLER_PORT));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                shared_ptr<GraphQueryAggregatorServiceClient> client(
                    new GraphQueryAggregatorServiceClient(protocol));
                transport->open();
                client->init();

                thread_data->client = client;
                thread_data->transport = transport;
              }
            }
        } catch (std::exception &e) {
            LOG_E("Throughput test ends...: '%s'\n", e.what());
        }
        return std::make_pair(query_thput, edges_thput);
    }

    void benchmark_tao_mix_throughput(
        const int num_threads,
        const std::string& master_hostname,
        const std::string& warmup_assoc_range_file,
        const std::string& assoc_range_file,
        const std::string& warmup_assoc_count_file,
        const std::string& assoc_count_file,
        const std::string& warmup_obj_get_file,
        const std::string& obj_get_file,
        const std::string& warmup_assoc_get_file,
        const std::string& assoc_get_file,
        const std::string& warmup_assoc_time_range_file,
        const std::string& assoc_time_range_file)
    {
        // assoc_range
        read_assoc_range_queries(warmup_assoc_range_file, assoc_range_file);
        // assoc_count
        read_neighbor_atype_queries(warmup_assoc_count_file, assoc_count_file,
            warmup_assoc_count_nodes, assoc_count_nodes,
            warmup_assoc_count_atypes, assoc_count_atypes);
        // obj_get
        read_neighbor_queries(warmup_obj_get_file, obj_get_file,
            warmup_obj_get_nodes, obj_get_nodes);
        // assoc_get
        read_assoc_get_queries(warmup_assoc_get_file, assoc_get_file);
        // assoc_time_range
        read_assoc_time_range_queries(
            warmup_assoc_time_range_file, assoc_time_range_file);

        bench_throughput(num_threads, master_hostname, BenchType::TAO_MIX);
    }

    void benchmark_tao_mix_with_updates_throughput(
        const int num_threads,
        const std::string& master_hostname,
        const std::string& warmup_assoc_range_file,
        const std::string& assoc_range_file,
        const std::string& warmup_assoc_count_file,
        const std::string& assoc_count_file,
        const std::string& warmup_obj_get_file,
        const std::string& obj_get_file,
        const std::string& warmup_assoc_get_file,
        const std::string& assoc_get_file,
        const std::string& warmup_assoc_time_range_file,
        const std::string& assoc_time_range_file)
    {
        // assoc_range
        read_assoc_range_queries(warmup_assoc_range_file, assoc_range_file);
        // assoc_count
        read_neighbor_atype_queries(warmup_assoc_count_file, assoc_count_file,
            warmup_assoc_count_nodes, assoc_count_nodes,
            warmup_assoc_count_atypes, assoc_count_atypes);
        // obj_get
        read_neighbor_queries(warmup_obj_get_file, obj_get_file,
            warmup_obj_get_nodes, obj_get_nodes);
        // assoc_get
        read_assoc_get_queries(warmup_assoc_get_file, assoc_get_file);
        // assoc_time_range
        read_assoc_time_range_queries(
            warmup_assoc_time_range_file, assoc_time_range_file);

        bench_throughput(num_threads, master_hostname,
            BenchType::TAO_MIX_WITH_UPDATES);
    }

    void benchmark_node_throughput(
        const int num_threads,
        const std::string& master_hostname,
        std::string warmup_query_file,
        std::string query_file)
    {
        read_node_queries(warmup_query_file, query_file);
        bench_throughput(num_threads, master_hostname, BenchType::NODE);
    }

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
        for(uint64_t i = 0; i < WARMUP_N; ++i) {
            get_nodes_f_(
                result, mod_get(warmup_node_attributes, i),
                mod_get(warmup_node_queries, i));
            assert(result.size() != 0 && "No result found in"
                " benchmarking node latency");
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for(uint64_t i = 0; i < MEASURE_N; ++i) {
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

    void benchmark_node_node_throughput(
        const int num_threads,
        const std::string& master_hostname,
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        read_node_queries(warmup_query_file, query_file);
        bench_throughput(num_threads, master_hostname, BenchType::NODE2);
    }

    void benchmark_node_node_latency(
        std::string res_path,
        uint64_t WARMUP_N,
        uint64_t MEASURE_N,
        const std::string& warmup_query_file,
        const std::string& query_file)
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
        for(uint64_t i = 0; i < WARMUP_N; ++i) {
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
        for(uint64_t i = 0; i < MEASURE_N; ++i) {
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

    void benchmark_tao_mix_latency(
        const std::string& assoc_range_res_file,
        const std::string& assoc_count_res_file,
        const std::string& obj_get_res_file,
        const std::string& assoc_get_res_file,
        const std::string& assoc_time_range_res_file,
        int warmup_n, int measure_n,
        const std::string& warmup_assoc_range_file,
        const std::string& assoc_range_file,
        const std::string& warmup_assoc_count_file,
        const std::string& assoc_count_file,
        const std::string& warmup_obj_get_file,
        const std::string& obj_get_file,
        const std::string& warmup_assoc_get_file,
        const std::string& assoc_get_file,
        const std::string& warmup_assoc_time_range_file,
        const std::string& assoc_time_range_file)
    {
        std::ofstream assoc_range_res(assoc_range_res_file);
        std::ofstream assoc_count_res(assoc_count_res_file);
        std::ofstream obj_get_res(obj_get_res_file);
        std::ofstream assoc_get_res(assoc_get_res_file);
        std::ofstream assoc_time_range_res(assoc_time_range_res_file);

        // assoc_range
        read_assoc_range_queries(warmup_assoc_range_file, assoc_range_file);
        // assoc_count
        read_neighbor_atype_queries(warmup_assoc_count_file, assoc_count_file,
            warmup_assoc_count_nodes, assoc_count_nodes,
            warmup_assoc_count_atypes, assoc_count_atypes);
        // obj_get
        read_neighbor_queries(warmup_obj_get_file, obj_get_file,
            warmup_obj_get_nodes, obj_get_nodes);
        // assoc_get
        read_assoc_get_queries(warmup_assoc_get_file, assoc_get_file);
        // assoc_time_range
        read_assoc_time_range_queries(
            warmup_assoc_time_range_file, assoc_time_range_file);

        thread_local std::mt19937 rng(1618);
        std::uniform_int_distribution<int> uni(0, 4);

        std::vector<ThriftAssoc> result;
        int64_t cnt;
        std::vector<std::string> attrs;
        time_t t0, t1;

        LOG_E("Benchmarking TAO mixed query latency\n");
        try {
            LOG_E("Warming up for %d queries...\n", warmup_n);
            for (int i = 0; i < warmup_n; ++i) {
                int rand_query = uni(rng);
                switch (rand_query) {
                case 0:
                    assoc_range_f_(result,
                        mod_get(warmup_assoc_range_nodes, i),
                        mod_get(warmup_assoc_range_atypes, i),
                        mod_get(warmup_assoc_range_offs, i),
                        mod_get(warmup_assoc_range_lens, i));
                    break;
                case 1:
                    assoc_count_f_(
                        mod_get(warmup_assoc_count_nodes, i),
                        mod_get(warmup_assoc_count_atypes, i));
                    break;
                case 2:
                    obj_get_f_(attrs, mod_get(warmup_obj_get_nodes, i));
                    break;
                case 3:
                    assoc_get_f_(result,
                        mod_get(warmup_assoc_get_nodes, i),
                        mod_get(warmup_assoc_get_atypes, i),
                        mod_get(warmup_assoc_get_dst_id_sets, i),
                        mod_get(warmup_assoc_get_lows, i),
                        mod_get(warmup_assoc_get_highs, i));
                    break;
                case 4:
                    assoc_time_range_f_(result,
                        mod_get(warmup_assoc_time_range_nodes, i),
                        mod_get(warmup_assoc_time_range_atypes, i),
                        mod_get(warmup_assoc_time_range_lows, i),
                        mod_get(warmup_assoc_time_range_highs, i),
                        mod_get(warmup_assoc_time_range_limits, i));
                    break;
                default:
                    assert(false);
                }
            }
            LOG_E("Warmup complete.\n");

            rng.seed(1618);

            // Measure phase
            LOG_E("Measuring for %d queries...\n", measure_n);
            for (int i = 0; i < measure_n; ++i) {
                int rand_query = uni(rng);
                switch (rand_query) {
                case 0:
                    t0 = get_timestamp();
                    assoc_range_f_(result,
                        mod_get(assoc_range_nodes, i),
                        mod_get(assoc_range_atypes, i),
                        mod_get(assoc_range_offs, i),
                        mod_get(assoc_range_lens, i));
                    t1 = get_timestamp();
                    assoc_range_res << result.size() << "," << t1 - t0 << '\n';
                    break;
                case 1:
                    t0 = get_timestamp();
                    cnt = assoc_count_f_(
                        mod_get(assoc_count_nodes, i),
                        mod_get(assoc_count_atypes, i));
                    t1 = get_timestamp();
                    assoc_count_res << cnt << "," << t1 - t0 << "\n";
                    break;
                case 2:
                    t0 = get_timestamp();
                    obj_get_f_(attrs, mod_get(obj_get_nodes, i));
                    t1 = get_timestamp();
                    obj_get_res << attrs.size() << "," << t1 - t0 << "\n";
                    break;
                case 3:
                    t0 = get_timestamp();
                    assoc_get_f_(result,
                        mod_get(assoc_get_nodes, i),
                        mod_get(assoc_get_atypes, i),
                        mod_get(assoc_get_dst_id_sets, i),
                        mod_get(assoc_get_lows, i),
                        mod_get(assoc_get_highs, i));
                    t1 = get_timestamp();
                    assoc_get_res << result.size() << "," << t1 - t0 << "\n";
                    break;
                case 4:
                    t0 = get_timestamp();
                    assoc_time_range_f_(result,
                        mod_get(assoc_time_range_nodes, i),
                        mod_get(assoc_time_range_atypes, i),
                        mod_get(assoc_time_range_lows, i),
                        mod_get(assoc_time_range_highs, i),
                        mod_get(assoc_time_range_limits, i));
                    t1 = get_timestamp();
                    assoc_time_range_res << result.size() << "," << t1 - t0
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

    void benchmark_tao_updates_latency(
        const std::string& assoc_add_res_file,
        int max_num_new_edges = MAX_NUM_NEW_EDGES)
    {
        assert(max_num_new_edges <= MAX_NUM_NEW_EDGES);

        std::ofstream assoc_add_res(assoc_add_res_file);
        std::mt19937 gen(1618);
        std::uniform_int_distribution<int64_t> dist_node(0, NUM_NODES - 1);
        std::uniform_int_distribution<int64_t> dist_atype(0, NUM_ATYPES - 1);

        time_t t0, t1;

        LOG_E("Benchmarking TAO assoc_add latency\n");
        int warmup_n = max_num_new_edges / 10;
        int measure_n = max_num_new_edges - warmup_n;
        int ret;
        int64_t src, atype, dst;

        try {
            LOG_E("Warming up for %d queries...\n", warmup_n);
            for (int i = 0; i < warmup_n; ++i) {
                src = dist_node(gen);
                atype = dist_atype(gen);
                dst = dist_node(gen);
                aggregator_->assoc_add(
                    src, atype, dst, MAX_TIME, ATTR_FOR_NEW_EDGES);
            }
            LOG_E("Warmup complete.\n");

            // Measure phase
            LOG_E("Measuring for %d queries...\n", measure_n);
            for (int i = 0; i < measure_n; ++i) {
                src = dist_node(gen);
                atype = dist_atype(gen);
                dst = dist_node(gen);

                t0 = get_timestamp();
                ret = aggregator_->assoc_add(
                    src, atype, dst, MAX_TIME, ATTR_FOR_NEW_EDGES);
                t1 = get_timestamp();
                assoc_add_res << ret << "," << t1 - t0 << "\n";
            }
            LOG_E("Measure complete.\n");
        } catch (std::exception &e) {
            LOG_E("Exception: %s\n", e.what());
        }
    }

    void benchmark_mix_throughput(
        const int num_threads,
        const std::string& master_hostname,
        const std::string& warmup_neighbor_query_file,
        const std::string& neighbor_query_file,
        const std::string& warmup_nhbr_atype_file,
        const std::string& nhbr_atype_file,
        const std::string& warmup_nhbr_node_file,
        const std::string& nhbr_node_file,
        const std::string& warmup_node_query_file,
        const std::string& node_query_file)
    {
        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file,
            warmup_neighbor_indices, neighbor_indices);
        read_neighbor_atype_queries(warmup_nhbr_atype_file, nhbr_atype_file,
            warmup_nhbrAtype_indices, nhbrAtype_indices,
            warmup_atypes, atypes);
        read_neighbor_node_queries(warmup_nhbr_node_file, nhbr_node_file);
        read_node_queries(warmup_node_query_file, node_query_file);

        bench_throughput(num_threads, master_hostname, BenchType::MIX);
    }

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

        read_neighbor_queries(warmup_neighbor_query_file, neighbor_query_file,
            warmup_neighbor_indices, neighbor_indices);
        read_neighbor_atype_queries(warmup_nhbr_atype_file, nhbr_atype_file,
            warmup_nhbrAtype_indices, nhbrAtype_indices,
            warmup_atypes, atypes);
        read_neighbor_node_queries(warmup_nhbr_node_file, nhbr_node_file);
        read_node_queries(warmup_node_query_file, node_query_file);

        thread_local std::mt19937 rng(1618);
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

    void benchmark_neighbor_node_throughput(
        const int num_threads,
        const std::string& master_hostname,
        std::string warmup_query_file,
        std::string query_file)
    {
        read_neighbor_node_queries(warmup_query_file, query_file);
        bench_throughput(num_threads, master_hostname, BenchType::NHBR_NODE);
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
        for(uint64_t i = 0; i < WARMUP_N; ++i) {
            get_neighbors_attr_f_(result,
                mod_get(warmup_nhbrNode_indices, i),
                mod_get(warmup_nhbrNode_attr_ids, i),
                mod_get(warmup_nhbrNode_attrs, i));
        }
        LOG_E("Warmup complete.\n");

        // Measure
        LOG_E("Measuring for %" PRIu64 " queries...\n", MEASURE_N);
        for (uint64_t i = 0; i < MEASURE_N; ++i) {
            t0 = get_timestamp();
            get_neighbors_attr_f_(result,
                mod_get(nhbrNode_indices, i),
                mod_get(nhbrNode_attr_ids, i),
                mod_get(nhbrNode_attrs, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            // correctness
            query_res_stream << "id " << mod_get(nhbrNode_indices, i)
                << " attr " << mod_get(nhbrNode_attr_ids, i);
            query_res_stream << " query " << mod_get(nhbrNode_attrs, i) << "\n";
            std::sort(result.begin(), result.end());
            for (auto it = result.begin(); it != result.end(); ++it)
                query_res_stream << *it << " ";
            query_res_stream << "\n";
#endif
        }

        LOG_E("Measure complete.\n");
    }

    void benchmark_assoc_range_latency(
        std::string res_path,
        uint64_t warmup_n,
        uint64_t measure_n,
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        read_assoc_range_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking assoc_range() latency\n");

        LOG_E("Warming up for %" PRIu64 " queries...\n", warmup_n);
        std::vector<ThriftAssoc> result;
        for (uint64_t i = 0; i < warmup_n; ++i) {
            assoc_range_f_(result,
                mod_get(warmup_assoc_range_nodes, i),
                mod_get(warmup_assoc_range_atypes, i),
                mod_get(warmup_assoc_range_offs, i),
                mod_get(warmup_assoc_range_lens, i));
        }
        LOG_E("Warmup complete.\n");

        LOG_E("Measuring for %" PRIu64 " queries...\n", measure_n);
        for (uint64_t i = 0; i < measure_n; ++i) {
            COND_LOG_E("query %lld\n", i);
            t0 = get_timestamp();
            assoc_range_f_(result,
                mod_get(assoc_range_nodes, i),
                mod_get(assoc_range_atypes, i),
                mod_get(assoc_range_offs, i),
                mod_get(assoc_range_lens, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            for (const auto& assoc : result) {
                query_res_stream
                    << "[src=" << assoc.srcId
                    << ",dst=" << assoc.dstId
                    << ",atype=" << assoc.atype
                    << ",time=" << assoc.timestamp
                    << ",attr='" << assoc.attr << "'] ";
            }
            query_res_stream << std::endl;
#endif
        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_assoc_count_latency(
        std::string res_path,
        uint64_t warmup_n,
        uint64_t measure_n,
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        read_neighbor_atype_queries(warmup_query_file, query_file,
            warmup_assoc_count_nodes, assoc_count_nodes,
            warmup_assoc_count_atypes, assoc_count_atypes);
        time_t t0, t1;
        std::ofstream res_stream(res_path);
        LOG_E("Benchmarking assoc_count() latency\n");

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Warming up for %" PRIu64 " queries...\n", warmup_n);
        std::vector<ThriftAssoc> result;
        for (uint64_t i = 0; i < warmup_n; ++i) {
            assoc_count_f_(
                mod_get(warmup_assoc_count_nodes, i),
                mod_get(warmup_assoc_count_atypes, i));
        }
        LOG_E("Warmup complete.\n");

        int64_t cnt;
        LOG_E("Measuring for %" PRIu64 " queries...\n", measure_n);
        for (uint64_t i = 0; i < measure_n; ++i) {
            t0 = get_timestamp();
            cnt = assoc_count_f_(
                mod_get(assoc_count_nodes, i),
                mod_get(assoc_count_atypes, i));
            t1 = get_timestamp();
            res_stream << cnt << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            query_res_stream << mod_get(assoc_count_nodes, i) << " "
                << mod_get(assoc_count_atypes, i) << " "
                << cnt << std::endl;
#endif

        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_assoc_get_latency(
        std::string res_path,
        int64_t warmup_n,
        int64_t measure_n,
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        read_assoc_get_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif
        LOG_E("Benchmarking assoc_get() latency\n");

        LOG_E("Warming up for %" PRIu64 " queries...\n", warmup_n);
        std::vector<ThriftAssoc> result;
        for (int64_t i = 0; i < warmup_n; ++i) {
            assoc_get_f_(result,
                mod_get(warmup_assoc_get_nodes, i),
                mod_get(warmup_assoc_get_atypes, i),
                mod_get(warmup_assoc_get_dst_id_sets, i),
                mod_get(warmup_assoc_get_lows, i),
                mod_get(warmup_assoc_get_highs, i));
        }
        LOG_E("Warmup complete.\n");

        LOG_E("Measuring for %" PRIu64 " queries...\n", measure_n);
        for (int64_t i = 0; i < measure_n; ++i) {
            t0 = get_timestamp();
            assoc_get_f_(result,
                mod_get(assoc_get_nodes, i),
                mod_get(assoc_get_atypes, i),
                mod_get(assoc_get_dst_id_sets, i),
                mod_get(assoc_get_lows, i),
                mod_get(assoc_get_highs, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            for (const auto& assoc : result) {
                query_res_stream
                    << "[src=" << assoc.srcId
                    << ",dst=" << assoc.dstId
                    << ",atype=" << assoc.atype
                    << ",time=" << assoc.timestamp
                    << ",attr='" << assoc.attr << "'] ";
            }
            query_res_stream << std::endl;
#endif
        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_obj_get_latency(
        std::string res_path,
        uint64_t warmup_n,
        uint64_t measure_n,
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        read_neighbor_queries(warmup_query_file, query_file,
            warmup_obj_get_nodes, obj_get_nodes);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif

        LOG_E("Benchmarking obj_get() latency\n");

        LOG_E("Warming up for %" PRIu64 " queries...\n", warmup_n);
        std::vector<std::string> result;
        for (uint64_t i = 0; i < warmup_n; ++i) {
            obj_get_f_(result, mod_get(warmup_obj_get_nodes, i));
        }
        LOG_E("Warmup complete.\n");

        LOG_E("Measuring for %" PRIu64 " queries...\n", measure_n);
        for (uint64_t i = 0; i < measure_n; ++i) {
            t0 = get_timestamp();
            obj_get_f_(result, mod_get(obj_get_nodes, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            for (const auto& attr : result) {
                query_res_stream << "'" << attr << "', ";
            }
            query_res_stream << std::endl;
#endif
        }
        LOG_E("Measure complete.\n");
    }

    void benchmark_assoc_time_range_latency(
        std::string res_path,
        int64_t warmup_n,
        int64_t measure_n,
        const std::string& warmup_query_file,
        const std::string& query_file)
    {
        read_assoc_time_range_queries(warmup_query_file, query_file);
        time_t t0, t1;
        std::ofstream res_stream(res_path);

#ifdef BENCH_PRINT_RESULTS
        std::ofstream query_res_stream(res_path + ".succinct_result");
#endif
        LOG_E("Benchmarking assoc_time_range() latency\n");

        LOG_E("Warming up for %" PRIu64 " queries...\n", warmup_n);
        std::vector<ThriftAssoc> result;
        for (int64_t i = 0; i < warmup_n; ++i) {
            assoc_time_range_f_(result,
                mod_get(warmup_assoc_time_range_nodes, i),
                mod_get(warmup_assoc_time_range_atypes, i),
                mod_get(warmup_assoc_time_range_lows, i),
                mod_get(warmup_assoc_time_range_highs, i),
                mod_get(warmup_assoc_time_range_limits, i));
        }
        LOG_E("Warmup complete.\n");

        LOG_E("Measuring for %" PRIu64 " queries...\n", measure_n);
        for (int64_t i = 0; i < measure_n; ++i) {
            t0 = get_timestamp();
            assoc_time_range_f_(result,
                mod_get(assoc_time_range_nodes, i),
                mod_get(assoc_time_range_atypes, i),
                mod_get(assoc_time_range_lows, i),
                mod_get(assoc_time_range_highs, i),
                mod_get(assoc_time_range_limits, i));
            t1 = get_timestamp();
            res_stream << result.size() << "," << t1 - t0 << "\n";

#ifdef BENCH_PRINT_RESULTS
            for (const auto& assoc : result) {
                query_res_stream
                    << "[src=" << assoc.srcId
                    << ",dst=" << assoc.dstId
                    << ",atype=" << assoc.atype
                    << ",time=" << assoc.timestamp
                    << ",attr='" << assoc.attr << "'] ";
            }
            query_res_stream << std::endl;
#endif
        }
        LOG_E("Measure complete.\n");
    }

protected:

    SuccinctGraph * graph_;
    shared_ptr<GraphQueryAggregatorServiceClient> aggregator_;
    shared_ptr<TTransport> transport_;

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

    // TAO functions

    std::function<void(std::vector<std::string>&, int64_t)> obj_get_f_;

    std::function<void(std::vector<ThriftAssoc>&,
        int64_t, int64_t, int32_t, int32_t)> assoc_range_f_;

    std::function<void(std::vector<ThriftAssoc>&,
        int64_t, int64_t,
        const std::set<int64_t>&, int64_t, int64_t)> assoc_get_f_;

    std::function<int64_t(int64_t, int64_t)> assoc_count_f_;

    std::function<void(std::vector<ThriftAssoc>&,
        int64_t, int64_t, int64_t, int64_t, int32_t)> assoc_time_range_f_;

    uint64_t WARMUP_N; uint64_t MEASURE_N;
    static const uint64_t COOLDOWN_N = 500;

    // get_nhbrs(n)
    std::vector<int64_t> warmup_neighbor_indices, neighbor_indices;

    // get_nhbrs(n, atype)
    std::vector<int64_t> warmup_nhbrAtype_indices, nhbrAtype_indices;
    std::vector<int64_t> warmup_atypes, atypes;

    // get_nhbrs(n, attr)
    std::vector<int64_t> warmup_nhbrNode_indices, nhbrNode_indices;
    std::vector<int> warmup_nhbrNode_attr_ids, nhbrNode_attr_ids;
    std::vector<std::string> warmup_nhbrNode_attrs, nhbrNode_attrs;

    // 2 get_nodes()
    std::vector<int> warmup_node_attributes, node_attributes;
    std::vector<std::string> warmup_node_queries, node_queries;
    std::vector<int> warmup_node_attributes2, node_attributes2;
    std::vector<std::string> warmup_node_queries2, node_queries2;

    // assoc_range()
    std::vector<int64_t> warmup_assoc_range_nodes, assoc_range_nodes;
    std::vector<int64_t> warmup_assoc_range_atypes, assoc_range_atypes;
    std::vector<int32_t> warmup_assoc_range_offs, assoc_range_offs;
    std::vector<int32_t> warmup_assoc_range_lens, assoc_range_lens;

    // assoc_count()
    std::vector<int64_t> warmup_assoc_count_nodes, assoc_count_nodes;
    std::vector<int64_t> warmup_assoc_count_atypes, assoc_count_atypes;

    // obj_get
    std::vector<int64_t> warmup_obj_get_nodes, obj_get_nodes;

    // assoc_get()
    std::vector<int64_t> warmup_assoc_get_nodes, assoc_get_nodes;
    std::vector<int64_t> warmup_assoc_get_atypes, assoc_get_atypes;
    std::vector<std::set<int64_t>> warmup_assoc_get_dst_id_sets;
    std::vector<std::set<int64_t>> assoc_get_dst_id_sets;
    std::vector<int64_t> warmup_assoc_get_highs, assoc_get_highs;
    std::vector<int64_t> warmup_assoc_get_lows, assoc_get_lows;

    // assoc_time_range()
    std::vector<int64_t> warmup_assoc_time_range_nodes, assoc_time_range_nodes;
    std::vector<int64_t> warmup_assoc_time_range_atypes;
    std::vector<int64_t> assoc_time_range_atypes;
    std::vector<int64_t> warmup_assoc_time_range_highs, assoc_time_range_highs;
    std::vector<int64_t> warmup_assoc_time_range_lows, assoc_time_range_lows;
    std::vector<int32_t> warmup_assoc_time_range_limits;
    std::vector<int32_t> assoc_time_range_limits;

    void read_assoc_range_queries(
        const std::string& warmup_file, const std::string& file)
    {
        auto read = [](
            const std::string& file,
            std::vector<int64_t>& nodes, std::vector<int64_t>& atypes,
            std::vector<int32_t>& offs, std::vector<int32_t>& lens)
        {
            std::ifstream ifs(file);
            std::string line, token;
            while (std::getline(ifs, line)) {
                std::stringstream ss(line);

                std::getline(ss, token, ',');
                nodes.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                atypes.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                offs.push_back(std::stoi(token));

                std::getline(ss, token);
                lens.push_back(std::stoll(token));
            }
        };
        read(warmup_file, warmup_assoc_range_nodes, warmup_assoc_range_atypes,
            warmup_assoc_range_offs, warmup_assoc_range_lens);

        read(file, assoc_range_nodes, assoc_range_atypes,
            assoc_range_offs, assoc_range_lens);
    }

    void read_assoc_get_queries(
        const std::string& warmup_file, const std::string& file)
    {
        auto read = [](
            const std::string& file,
            std::vector<int64_t>& nodes, std::vector<int64_t>& atypes,
            std::vector<int64_t>& lows, std::vector<int64_t>& highs,
            std::vector<std::set<int64_t>>& dst_id_sets)
        {
            std::ifstream ifs(file);
            std::string line, token;
            while (std::getline(ifs, line)) {
                std::stringstream ss(line);

                std::getline(ss, token, ',');
                nodes.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                atypes.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                lows.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                highs.push_back(std::stoll(token));

                std::set<int64_t> dst_id_set;
                while (std::getline(ss, token, ',')) {
                    dst_id_set.insert(std::stoll(token));
                }
                dst_id_sets.push_back(dst_id_set);
            }
        };
        read(warmup_file, warmup_assoc_get_nodes, warmup_assoc_get_atypes,
            warmup_assoc_get_lows, warmup_assoc_get_highs,
            warmup_assoc_get_dst_id_sets);

        read(file, assoc_get_nodes, assoc_get_atypes,
            assoc_get_lows, assoc_get_highs, assoc_get_dst_id_sets);
    }

    void read_assoc_time_range_queries(
        const std::string& warmup_file, const std::string& file)
    {
        auto read = [](
            const std::string& file,
            std::vector<int64_t>& nodes, std::vector<int64_t>& atypes,
            std::vector<int64_t>& lows, std::vector<int64_t>& highs,
            std::vector<int32_t>& limits)
        {
            std::ifstream ifs(file);
            std::string line, token;
            while (std::getline(ifs, line)) {
                std::stringstream ss(line);

                std::getline(ss, token, ',');
                nodes.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                atypes.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                lows.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                highs.push_back(std::stoll(token));

                std::getline(ss, token, ',');
                limits.push_back(std::stoi(token));
            }
        };
        read(warmup_file, warmup_assoc_time_range_nodes,
            warmup_assoc_time_range_atypes,
            warmup_assoc_time_range_lows, warmup_assoc_time_range_highs,
            warmup_assoc_time_range_limits);

        read(file, assoc_time_range_nodes, assoc_time_range_atypes,
            assoc_time_range_lows, assoc_time_range_highs,
            assoc_time_range_limits);
    }

    void read_neighbor_queries(
        const std::string& warmup_neighbor_file,
        const std::string& query_neighbor_file,
        std::vector<int64_t>& warmup_neighbor_indices,
        std::vector<int64_t>& neighbor_indices)
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
        const std::string& warmup_file, const std::string& query_file,
        std::vector<int64_t>& warmup_nhbrAtype_indices,
        std::vector<int64_t>& nhbrAtype_indices,
        std::vector<int64_t>& warmup_atypes,
        std::vector<int64_t>& atypes)
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
