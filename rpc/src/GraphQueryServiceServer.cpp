#include "GraphQueryService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "GraphFormatter.hpp"
#include "GraphLogStore.h"
#include "GraphSuffixStore.h"
#include "SuccinctGraph.hpp"
#include "utils.h"
#include "ports.h"

#include <random>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <boost/functional/hash.hpp>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

// ******************** hacks: we should really make sure these
// states are not flying around outside of a class.

std::mutex succinct_store_mutex;
shared_ptr<SuccinctGraph> graph_(new SuccinctGraph(""));
bool succinct_store_initialized = false;

std::mutex global_init_mutex;
bool initialized_ = false;

// Updates

// src -> (atype -> [shard id, file offset])
std::unordered_map<int64_t,
    std::unordered_map<int64_t, std::vector<ThriftEdgeUpdatePtr>>
> edge_update_ptrs;
std::mutex edge_update_ptrs_mutex;

// src -> (shard id, file offset)
typedef std::pair<int, int64_t> NodeUpdatePtr;
std::unordered_map<int64_t, NodeUpdatePtr> node_update_ptrs;

shared_ptr<GraphLogStore> graph_log_store_ = nullptr;
shared_ptr<GraphSuffixStore> graph_suffix_store_ = nullptr;

std::mutex suffix_store_mutex, log_store_mutex;
bool suffix_store_initialized = false, log_store_initialized = false;

// old shard id -> set { (src, atype) }
std::unordered_map<int, GraphFormatter::AssocSet> edge_updates;
// ********************************** hacks done

class GraphQueryServiceHandler : virtual public GraphQueryServiceIf {
public:

    GraphQueryServiceHandler(
        const std::string& node_file,
        const std::string& edge_file,
        bool construct,
        int32_t sa_sampling_rate,
        int32_t isa_sampling_rate,
        int32_t npa_sampling_rate,
        int shard_id,
        int total_num_shards,
        const StoreMode store_mode = StoreMode::SuccinctStore)
    : shard_id_(shard_id),
      total_num_shards_(total_num_shards),
      node_file_(node_file),
      edge_file_(edge_file),
      construct_(construct),
      store_mode_(store_mode)
    {
        {
            std::lock_guard<std::mutex> lk(succinct_store_mutex);
            graph_->set_npa_sampling_rate(npa_sampling_rate);
            graph_->set_sa_sampling_rate(sa_sampling_rate);
            graph_->set_isa_sampling_rate(isa_sampling_rate);
        }

        if (construct) {
            node_table_empty_ = !file_or_dir_exists(node_file);
            edge_table_empty_ = !file_or_dir_exists(edge_file);
        } else {
            std::string suffix;
            switch (store_mode_) {
            case StoreMode::SuccinctStore:
                suffix = ".succinct";
                break;
            case StoreMode::SuffixStore:
                suffix = "_suffixstore";
                break;
            case StoreMode::LogStore:
                suffix = "_logstore";
                break;
            }
            node_table_empty_ = !file_or_dir_exists(node_file + suffix);
            edge_table_empty_ = !file_or_dir_exists(edge_file + suffix);
        }

        LOG_E(
            "shard id %d, total num shards %d; isa %d, sa %d, npa %d"
            " (specified, please check actual)\n",
            shard_id_, total_num_shards_, isa_sampling_rate,
            sa_sampling_rate, npa_sampling_rate);
        LOG_E("Store mode: %d\n", store_mode);
    }

    // Loads or constructs graph shards.
    int32_t init() {
        std::lock_guard<std::mutex> lk(global_init_mutex);
        if (initialized_) {
            LOG_E("Already initialized\n");
            return 0;
        }
        LOG_E("In shard %d's init()\n", shard_id_);

        switch (store_mode_) {
        case StoreMode::SuccinctStore:
            {
                std::lock_guard<std::mutex> lock(succinct_store_mutex);
                if (succinct_store_initialized) {
                    break;
                }
                if (construct_) {
                    LOG_E("Construct is set to true:"
                        " starting to construct & encode\n");
                    if (!node_table_empty_ && !edge_table_empty_) {
                        graph_->construct(node_file_, edge_file_); // in parallel
                    } else if (!node_table_empty_) {
                        graph_->construct_node_table(node_file_);
                    } else if (!edge_table_empty_) {
                        graph_->construct_edge_table(edge_file_);
                    } else {
                        assert(false && "Neither node file nor edge file exists!");
                    }
                } else {
                    LOG_E("Construct is set to false: starting to load\n");
                    if (!node_table_empty_ && !edge_table_empty_) {
                        graph_->load(node_file_, edge_file_);
                    } else if (!node_table_empty_) {
                        graph_->load_node_table(node_file_);
                    } else if (!edge_table_empty_) {
                        graph_->load_edge_table(edge_file_);
                    } else {
                        assert(false && "Neither node file nor edge file exists!");
                    }
                }
                succinct_store_initialized = true;
            }
            break;

        case StoreMode::SuffixStore:
            {
                std::lock_guard<std::mutex> lock(suffix_store_mutex);
                if (suffix_store_initialized) {
                    break;
                }
                graph_suffix_store_ = shared_ptr<GraphSuffixStore>(
                    new GraphSuffixStore(node_file_, edge_file_));
                if (construct_) {
                    graph_suffix_store_->construct();
                } else {
                    graph_suffix_store_->load();
                }
                LOG_E("Calculating backfill edge updates...\n");
                graph_suffix_store_->build_backfill_edge_updates(
                    edge_updates, total_num_shards_); // num succinct st. shards
                suffix_store_initialized = true;
            }
            break;

        case StoreMode::LogStore:
            {
                std::lock_guard<std::mutex> lock(log_store_mutex);
                if (log_store_initialized)  {
                    break;
                }
                graph_log_store_ = shared_ptr<GraphLogStore>(new GraphLogStore(
                    node_file_, edge_file_));
                if (construct_) {
                    graph_log_store_->construct();
                } else {
                    graph_log_store_->load();
                }
                LOG_E("Calculating backfill edge updates...\n");
                graph_log_store_->build_backfill_edge_updates(
                    edge_updates, total_num_shards_); // num succinct st. shards
                log_store_initialized = true;
            }
            break;

        default:
            break;
        }
        initialized_ = true;
        LOG_E("Initialization at this shard: done\n");
        return 0;
    }

    void get_edge_updates(
        std::map<int32_t, std::vector<ThriftSrcAtype> >& _return)
    {
        _return.clear();
        ThriftSrcAtype tsa;
        for (auto it = edge_updates.begin(); it != edge_updates.end(); ++it) {
            auto& assoc_set = it->second;
            auto& set = _return[it->first];

            for (auto it2 = assoc_set.begin(); it2 != assoc_set.end(); ++it2) {
                tsa.src = it2->first;
                tsa.atype = it2->second;
                set.push_back(tsa);
            }
        }
    }

    void record_edge_updates(
        const int32_t next_shard_id, const std::vector<ThriftSrcAtype> & updates)
    {
        LOG_E("Recording %lld updates from nextShard %d\n",
            updates.size(), next_shard_id);

        std::lock_guard<std::mutex> lk(edge_update_ptrs_mutex);
        ThriftEdgeUpdatePtr ptr;

        for (auto& update : updates) {
            ptr.shardId = next_shard_id;
            ptr.offset = -1; // TODO: offset optimization is not implemented yet
            edge_update_ptrs[update.src][update.atype].push_back(ptr);
        }
    }

    // In principle, nodeId should be in this shard's edge table.
    void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
        // Your implementation goes here
        COND_LOG_E("Received: get_neighbors(%lld)\n", nodeId);

        assert(nodeId % total_num_shards_ == shard_id_);
        if (edge_table_empty_) {
            _return.clear();
            return;
        }
#ifdef DEBUG_RPC_NHBR
        auto t1 = get_timestamp();
#endif
        graph_->get_neighbors(_return, nodeId);

#ifdef DEBUG_RPC_NHBR
        auto t2 = get_timestamp();
        if (shard_id_ == 0) {
            LOG_E(",%lld\n", t2 - t1);
        }
#endif
    }

    void get_neighbors_atype(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        COND_LOG_E("get_neighbors_atype\n");

        assert(nodeId % total_num_shards_ == shard_id_);
        if (edge_table_empty_) {
            _return.clear();
            return;
        }
#ifdef DEBUG_RPC_NHBR
        auto t1 = get_timestamp();
#endif
        graph_->get_neighbors(_return, nodeId, atype);

#ifdef DEBUG_RPC_NHBR
        auto t2 = get_timestamp();
        if (shard_id_ == 0) {
            LOG_E(",%lld\n", t2 - t1);
        }
#endif
    }

    void get_edge_attrs(
        std::vector<std::string> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        COND_LOG_E("get_edge_attrs\n");
        assert(nodeId % total_num_shards_ == shard_id_);
        if (edge_table_empty_) {
            _return.clear();
            return;
        }
        graph_->get_edge_attrs(_return, nodeId, atype);
    }

    void get_nodes(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("get_nodes\n");

        _return.clear();
        if (node_table_empty_) {
            return;
        }

        std::set<int64_t> local_keys;
        graph_->get_nodes(local_keys, attrId, attrKey);

        // TODO: this assumes a particular form of hash partitioning
        auto it = _return.begin();
        for (int64_t local_key : local_keys) {
            it = _return.insert(it, local_key * total_num_shards_ + shard_id_);
        }
    }

    void get_nodes2(
        std::set<int64_t> & _return,
        const int32_t attrId1,
        const std::string& attrKey1,
        const int32_t attrId2,
        const std::string& attrKey2)
    {
        COND_LOG_E("get_nodes2\n");

        _return.clear();
        if (node_table_empty_) {
            return;
        }

        std::set<int64_t> local_keys;
        graph_->get_nodes(local_keys, attrId1, attrKey1, attrId2, attrKey2);

        // TODO: this assumes a particular form of hash partitioning
        auto it = _return.begin();
        for (int64_t local_key : local_keys) {
            it = _return.insert(it, local_key * total_num_shards_ + shard_id_);
        }
    }

    void get_attribute_local(
        std::string& _return,
        const int64_t nodeId,
        const int32_t attrId)
    {
        graph_->get_attribute(_return, nodeId, attrId);
    }

    void filter_nodes(
        std::vector<int64_t> & _return,
        const std::vector<int64_t> & nodeIds,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("filter_nodes received\n");
        graph_->filter_nodes(_return, nodeIds, attrId, attrKey);
    }

    void assoc_range(
        std::vector<ThriftAssoc>& _return,
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len)
    {
        std::vector<SuccinctGraph::Assoc> vec;
        switch (store_mode_) {
        case StoreMode::SuccinctStore:
            vec = std::move(graph_->assoc_range(src, atype, off, len));
            break;
        case StoreMode::SuffixStore:
            vec = std::move(
                graph_suffix_store_->assoc_range(src, atype, off, len));
            break;
        case StoreMode::LogStore:
            vec = std::move(
                graph_log_store_->assoc_range(src, atype, off, len));
            break;
        }

        // TODO: any better way?
        // NB: the fields are Thrift-generated, so this may not be portable.
        _return.clear();
        _return.resize(vec.size());
        size_t i = 0;
        for (const auto& assoc : vec) {
            _return[i].srcId = assoc.src_id;
            _return[i].dstId = assoc.dst_id;
            _return[i].atype = assoc.atype;
            _return[i].timestamp = assoc.time;
            // should be the same as 'lhs = std::move(rhs)'
            _return[i].attr.assign(std::move(assoc.attr));
            ++i;
        }
    }

    int64_t assoc_count(int64_t src, int64_t atype) {
        switch (store_mode_) {
        case StoreMode::SuccinctStore:
            return graph_->assoc_count(src, atype);
        case StoreMode::SuffixStore:
            return graph_suffix_store_->assoc_count(src, atype);
        case StoreMode::LogStore:
            return graph_log_store_->assoc_count(src, atype);
        }
    }

    void assoc_get(
        std::vector<ThriftAssoc>& _return,
        const int64_t src,
        const int64_t atype,
        const std::set<int64_t>& dstIdSet,
        const int64_t tLow,
        const int64_t tHigh)
    {
        std::vector<SuccinctGraph::Assoc> vec;
        switch (store_mode_) {
        case StoreMode::SuccinctStore:
            COND_LOG_E("in shard assoc_get, about to call graph\n");
            vec = std::move(
                graph_->assoc_get(src, atype, dstIdSet, tLow, tHigh));
            COND_LOG_E("done: in shard assoc_get, about to call graph\n");
            break;

        case StoreMode::SuffixStore:
            vec = std::move(
                graph_suffix_store_->assoc_get(
                    src, atype, dstIdSet, tLow, tHigh));
            break;

        case StoreMode::LogStore:
            vec = std::move(
                graph_log_store_->assoc_get(src, atype, dstIdSet, tLow, tHigh));
            break;
        }

        // TODO: any better way?
        // NB: the fields are Thrift-generated, so this may not be portable.
        _return.clear();
        _return.resize(vec.size());
        size_t i = 0;
        for (const auto& assoc : vec) {
            _return[i].srcId = assoc.src_id;
            _return[i].dstId = assoc.dst_id;
            _return[i].atype = assoc.atype;
            _return[i].timestamp = assoc.time;
            // should be the same as 'lhs = std::move(rhs)'
            _return[i].attr.assign(std::move(assoc.attr));
            ++i;
        }
    }

    void obj_get(std::vector<std::string>& _return, const int64_t local_id) {
        switch (store_mode_) {
        case StoreMode::SuccinctStore:
            graph_->obj_get(_return, local_id);
            break;

        case StoreMode::SuffixStore:
            graph_suffix_store_->obj_get(_return, local_id);
            break;

        case StoreMode::LogStore:
            graph_log_store_->obj_get(_return, local_id);
            break;
        }
    }

    void assoc_time_range(
        std::vector<ThriftAssoc>& _return,
        const int64_t src,
        const int64_t atype,
        const int64_t tLow,
        const int64_t tHigh,
        const int32_t limit)
    {
        std::vector<SuccinctGraph::Assoc> vec;
        switch (store_mode_) {
        case StoreMode::SuccinctStore:
            vec = std::move(
                graph_->assoc_time_range(src, atype, tLow, tHigh, limit));
            break;

        case StoreMode::SuffixStore:
            vec = std::move(
                graph_suffix_store_->assoc_time_range(
                    src, atype, tLow, tHigh, limit));
            break;

        case StoreMode::LogStore:
            vec = std::move(
                graph_log_store_->assoc_time_range(
                    src, atype, tLow, tHigh, limit));
            break;
        }

        // TODO: any better way?
        // NB: the fields are Thrift-generated, so this may not be portable.
        _return.clear();
        _return.resize(vec.size());
        size_t i = 0;
        for (const auto& assoc : vec) {
            _return[i].srcId = assoc.src_id;
            _return[i].dstId = assoc.dst_id;
            _return[i].atype = assoc.atype;
            _return[i].timestamp = assoc.time;
            // should be the same as 'lhs = std::move(rhs)'
            _return[i].attr.assign(std::move(assoc.attr));
            ++i;
        }
    }

    void get_edge_update_ptrs(
        std::vector<ThriftEdgeUpdatePtr> & _return,
        const int64_t src,
        const int64_t atype)
    {
        _return = edge_update_ptrs[src][atype];
    }

private:

    // By default, StoreMode::SuccinctStore
    const StoreMode store_mode_;

    const int shard_id_;
    const int total_num_shards_;

    const std::string node_file_;
    const std::string edge_file_;
    const bool construct_;

    bool node_table_empty_ = true;
    bool edge_table_empty_ = true;

};


int main(int argc, char **argv) {
    if (argc < 2) {
        LOG_E("Exiting due to argc < 2\n");
        return -1;
    }
    LOG_E("Command line: ");
    for (int i = 0; i < argc; i++) LOG_E("%s ", argv[i]);
    LOG_E("\n");

    int c;
    int mode = 0, port = QUERY_SERVER_PORT;
    // default Succinct Graph level 0
    int sa_sampling_rate = 32, isa_sampling_rate = 64, npa_sampling_rate = 128;
    int shard_id = 0, total_num_shards = 1;
    int local_host_id = 0, total_num_hosts = 1;
    StoreMode store_mode = StoreMode::SuccinctStore; // by default, Succinct
    bool multistore_enabled = false;

    while ((c = getopt(argc, argv, "m:p:s:i:n:t:d:h:k:b:")) != -1) {
        switch (c) {
        case 'm':
            mode = atoi(optarg); // 0 for construct, 1 for load
            break;
        case 'p':
            port = atoi(optarg); // port for this shard server
            break;
        case 's':
            sa_sampling_rate = atoi(optarg);
            break;
        case 'i':
            isa_sampling_rate = atoi(optarg);
            break;
        case 'n':
            npa_sampling_rate = atoi(optarg);
            break;
        case 't':
            total_num_shards = atoi(optarg);
            break;
        case 'd':
            shard_id = atoi(optarg);
            break;
        case 'h':
            local_host_id = atoi(optarg);
            break;
        case 'k':
            total_num_hosts = atoi(optarg);
            break;
        case 'b':
            LOG_E("multistore_enabled argument: '%s'\n", optarg);
            multistore_enabled = (std::string(optarg) == "T");
            break;
        }
    }
    if (multistore_enabled) {
        LOG_E("Multistore enabled!\n");
    } else {
        LOG_E("Multistore disabled, default to SuccinctStore\n");
    }

    // Hack for multistore setup:
    if (multistore_enabled && total_num_hosts >= 3) {
        if (local_host_id == total_num_hosts - 1) {
            COND_LOG_E("Setting shard's store mode to LogStore\n");
            store_mode = StoreMode::LogStore;
        } else if (local_host_id == total_num_hosts - 2) {
            COND_LOG_E("Setting shard's store mode to SuffixStore\n");
            store_mode = StoreMode::SuffixStore;
        }
    }

    if (optind + 1 >= argc) return -1;
    std::string node_file = std::string(argv[optind]);
    std::string edge_file = std::string(argv[optind + 1]);
    bool construct = (mode == 0) ? true : false;

    try {
        shared_ptr<GraphQueryServiceHandler> handler(
            new GraphQueryServiceHandler(
                node_file,
                edge_file,
                construct,
                sa_sampling_rate,
                isa_sampling_rate,
                npa_sampling_rate,
                shard_id,
                total_num_shards,
                store_mode));

        shared_ptr<TProcessor> processor(
            new GraphQueryServiceProcessor(handler));
        shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
        shared_ptr<TTransportFactory> transportFactory(
            new TBufferedTransportFactory());
        shared_ptr<TProtocolFactory> protocolFactory(
            new TBinaryProtocolFactory());
        // TODO: simple server vs. threaded server?
        TThreadedServer server(
            processor, serverTransport, transportFactory, protocolFactory);

        server.serve();
    } catch (std::exception& e) {
        fprintf(stderr,
                "Exception at GraphQueryServiceServer:main(): %s\n",
                e.what());
    }
    return 0;
}
