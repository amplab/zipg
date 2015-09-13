#include "GraphQueryAggregatorService.h"

#include <fstream>
#include <unordered_map>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>

#include "ports.h"
#include "utils.h"
#include "GraphQueryService.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

class GraphQueryAggregatorServiceHandler :
    virtual public GraphQueryAggregatorServiceIf {

public:
    GraphQueryAggregatorServiceHandler(
        int total_num_shards,
        int local_num_shards,
        int local_host_id,
        const std::vector<std::string>& hostnames)
    : total_num_shards_(total_num_shards),
      local_num_shards_(local_num_shards),
      local_host_id_(local_host_id),
      hostnames_(hostnames),
      total_num_hosts_(hostnames.size())
    { }

    int32_t init() {
        connect_to_local_shards();
        if (connect_to_aggregators() != 0) {
            LOG_E("Connection to remote aggregators not successful!\n");
            exit(1);
        }

        for (auto& shard : local_shards_) {
            shard.send_init();
        }

        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).connect_to_aggregators();
        }

        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).send_init_local_shards();
        }

        for (auto& shard : local_shards_) {
            if (shard.recv_init() != 0) {
                LOG_E("Some shard doesn't init() successfully, exiting\n");
                exit(1);
            }
        }

        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            if (aggregators_.at(i).recv_init_local_shards() != 0) {
                LOG_E("Some aggregator doesn't init_local_shards() successfully"
                    ", exiting\n");
                exit(1);
            }
        }
        LOG_E("Cluster init() done\n");
        return 0;
    }

    int32_t init_local_shards() {
        LOG_E("About to connect to local shards and init them\n");
        if (connect_to_local_shards() != 0) {
            LOG_E("Connection to local shards failed\n");
            exit(1);
        }
        for (auto& shard : local_shards_) {
            shard.send_init();
        }
        for (auto& shard : local_shards_) {
            if (shard.recv_init() != 0) {
                LOG_E("Some shard doesn't init() successfully, exiting\n");
                exit(1);
            }
        }
        LOG_E("init_local_shards() done\n");
        return 0;
    }

    int32_t connect_to_aggregators() {
        aggregators_.clear();

        for (int i = 0; i < hostnames_.size(); ++i) { // FIXME: total_num_hosts_?
            if (i == local_host_id_) {
                continue;
            }

            LOG_E("Connecting to remote aggregator on host %d...\n", i);
            try {
                shared_ptr<TSocket> socket(new TSocket(
                    hostnames_.at(i), QUERY_HANDLER_PORT));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                GraphQueryAggregatorServiceClient client(protocol);

                transport->open();
                LOG_E("Connected!\n");
                aggregators_.insert(
                    std::pair<int, GraphQueryAggregatorServiceClient>(
                        i, client));
            } catch (std::exception& e) {
                LOG_E("Could not connect to aggregator %d: %s\n", i, e.what());
                return 1;
            }
        }
        if (hostnames_.size() != total_num_hosts_) {
            LOG_E("%zu total aggregators, but only %zu live\n",
                total_num_hosts_, hostnames_.size());
            return 1;
        }
        LOG_E("Aggregators connected: cluster has %zu aggregators in total.\n",
            hostnames_.size());
        return 0;
    }

private:

    int32_t connect_to_local_shards() {
        for (int i = 0; i < local_num_shards_; ++i) {
            // Desirable? Hacky way to facilitate benchmark client reconnecting
            // to a healthy cluster of aggregators & shard servers.
            if (i < local_shards_.size()) continue;

            LOG_E("Connecting to local server %d...", i);
            try {
                shared_ptr<TSocket> socket(new TSocket(
                    "localhost", QUERY_SERVER_PORT + i));
                shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
                shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                GraphQueryServiceClient client(protocol);

                transport->open();
                LOG_E("Connected!\n");
                local_shards_.push_back(client);
            } catch (std::exception& e) {
                LOG_E("Could not connect to server: %s\n", e.what());
                return 1;
            }
        }
        LOG_E(
            "Currently have %zu local server connections.\n",
            local_shards_.size());
        return 0;
    }

public:

    void get_attribute_local(
        std::string& _return,
        const int64_t nodeId,
        const int32_t attrId)
    {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id != local_host_id_) {
            LOG_E("get_attribute()'s internal for now: supports 1 agg. only\n");
            exit(1);
        }
        local_shards_.at(shard_id / total_num_hosts_).get_attribute_local(
            _return, global_to_local_node_id(nodeId, shard_id), attrId);
    }

    void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        COND_LOG_E(
            "Received: get_neighbors(%lld), route to shard %d on host %d\n",
            nodeId, shard_id, host_id);
        if (host_id == local_host_id_) {

#ifdef DEBUG_RPC_NHBR
            auto t1 = get_timestamp();
#endif

            local_shards_[shard_id / total_num_hosts_]
                .get_neighbors(_return, nodeId);

#ifdef DEBUG_RPC_NHBR
            auto t2 = get_timestamp();
            if (shard_id == 0) {
                LOG_E(".%lld\n", t2 - t1);
            }
#endif

        } else {
            aggregators_.at(host_id).get_neighbors_local(
                _return, shard_id, nodeId);
        }
    }

    void get_neighbors_local(
        std::vector<int64_t> & _return,
        const int32_t shardId,
        const int64_t nodeId)
    {
        local_shards_[shardId / total_num_hosts_]
            .get_neighbors(_return, nodeId);
    }

    void get_neighbors_atype(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
#ifdef DEBUG_RPC_NHBR
            auto t1 = get_timestamp();
#endif
            local_shards_[shard_id / total_num_hosts_]
                .get_neighbors_atype(_return, nodeId, atype);

#ifdef DEBUG_RPC_NHBR
            auto t2 = get_timestamp();
            if (shard_id == 0) {
                LOG_E(".%lld\n", t2 - t1);
            }
#endif
        } else {
            aggregators_.at(host_id).get_neighbors_atype_local(
                _return, shard_id, nodeId, atype);
        }
    }

    void get_neighbors_atype_local(
        std::vector<int64_t> & _return,
        const int32_t shardId,
        const int64_t nodeId,
        const int64_t atype)
    {
        local_shards_[shardId / total_num_hosts_]
            .get_neighbors_atype(_return, nodeId, atype);
    }

    void get_neighbors_attr(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("Aggregator get_nhbr_node(nodeId %d, attrId %d)\n",
            nodeId, attrId);

        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        // Delegate to the shard responsible for nodeId.
        if (host_id == local_host_id_) {
            COND_LOG_E("Delegating to myself\n");

            get_neighbors_attr_local(
                _return, shard_id, nodeId, attrId, attrKey);
        } else {
            COND_LOG_E("Route to aggregator on host %d\n", host_id);

            aggregators_.at(host_id).get_neighbors_attr_local(
                _return, shard_id, nodeId, attrId, attrKey);
        }
    }

    // TODO: rid of shardId? Maybe more expensive to ship an int over network?
    void get_neighbors_attr_local(
        std::vector<int64_t> & _return,
        const int32_t shardId,
        const int64_t nodeId,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("In get_nhbr_node_local(shardId %d, nodeId %d, attrId %d)\n",
            shardId, nodeId, attrId);

        std::vector<int64_t> nhbrs;
        get_neighbors_local(nhbrs, shardId, nodeId);
        COND_LOG_E("nhbrs size: %d\n", nhbrs.size());

        // hostId -> [list of responsible nhbr IDs to check]
        std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
        int host_id;

        for (int64_t nhbr_id : nhbrs) {
            host_id = (nhbr_id % total_num_shards_) % total_num_hosts_;
            splits_by_keys[host_id].push_back(nhbr_id); // global
        }

        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            host_id = it->first;
            if (host_id == local_host_id_) {
                filter_nodes_local(_return, it->second, attrId, attrKey);
                COND_LOG_E("locally filtered result: %d\n", _return.size());
            } else {
                COND_LOG_E("host id %d\n", host_id);
                aggregators_.at(host_id).send_filter_nodes_local(
                    it->second, attrId, attrKey);
            }
        }

        std::vector<int64_t> shard_result;
        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            host_id = it->first;
            // The equal case has already been computed in loop above
            if (host_id != local_host_id_) {
                aggregators_.at(host_id).recv_filter_nodes_local(shard_result);
                COND_LOG_E("remotely filtered result: %d\n", shard_result.size());
            }
            _return.insert(
                _return.end(), shard_result.begin(), shard_result.end());
        }
    }

    void filter_nodes_local(
        std::vector<int64_t>& _return,
        const std::vector<int64_t>& nodeIds,
        const int32_t attrId,
        const std::string& attrKey)
    {
        COND_LOG_E("in agg. filter_nodes_local, %d ids to filter\n",
            nodeIds.size());
        // shardId -> [list of responsible nhbr IDs to check]
        std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
        int shard_id;

        for (int64_t nhbr_id : nodeIds) {
            shard_id = nhbr_id % total_num_shards_;
            splits_by_keys[shard_id].push_back(
                global_to_local_node_id(nhbr_id, shard_id)); // to local
        }

        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            COND_LOG_E("sending to shard %d, filter_nodes\n",
                it->first / total_num_hosts_);
            // FIXME?: try to sleep a while? get_nhbr(n, attr) bug here?
            local_shards_[it->first / total_num_hosts_]
                .send_filter_nodes(it->second, attrId, attrKey);
        }

        _return.clear();
        std::vector<int64_t> shard_result;
        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            COND_LOG_E("receiving filter_nodes() result from shard %d, ",
                it->first / total_num_hosts_);
            local_shards_[it->first / total_num_hosts_]
                .recv_filter_nodes(shard_result);
            COND_LOG_E("size: %d\n", shard_result.size());
            // local back to global
            for (const int64_t local_key : shard_result) {
                // globalKey = localKey * numShards + shardId
                // localKey = (globalKey - shardId) / numShards
                _return.push_back(local_key * total_num_shards_ + it->first);
            }
        }
    }

    void get_nodes(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).send_get_nodes_local(attrId, attrKey);
        }

        get_nodes_local(_return, attrId, attrKey);

        std::set<int64_t> shard_result;
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).recv_get_nodes_local(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void get_nodes_local(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        for (auto& shard : local_shards_) {
            shard.send_get_nodes(attrId, attrKey);
        }

        std::set<int64_t> shard_result;
        _return.clear();

        for (auto& shard : local_shards_) {
            shard.recv_get_nodes(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void get_nodes2(
        std::set<int64_t> & _return,
        const int32_t attrId1,
        const std::string& attrKey1,
        const int32_t attrId2,
        const std::string& attrKey2)
    {
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).send_get_nodes2_local(
                attrId1, attrKey1, attrId2, attrKey2);
        }

        get_nodes2_local(_return, attrId1, attrKey1, attrId2, attrKey2);

        std::set<int64_t> shard_result;
        for (int i = 0; i < total_num_hosts_; ++i) {
            if (i == local_host_id_) {
                continue;
            }
            aggregators_.at(i).recv_get_nodes2_local(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void get_nodes2_local(
        std::set<int64_t> & _return,
        const int32_t attrId1,
        const std::string& attrKey1,
        const int32_t attrId2,
        const std::string& attrKey2)
    {
        for (auto& shard : local_shards_) {
            shard.send_get_nodes2(attrId1, attrKey1, attrId2, attrKey2);
        }

        std::set<int64_t> shard_result;
        _return.clear();

        for (auto& shard : local_shards_) {
            shard.recv_get_nodes2(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

    void assoc_range(
        std::vector<ThriftAssoc>& _return,
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len)
    {
        COND_LOG_E("in aggregator assoc_range\n");
        int shard_id = src % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            local_shards_[shard_id / total_num_hosts_]
                .assoc_range(_return, src, atype, off, len);
        } else {
            aggregators_.at(host_id).assoc_range_local(
                _return, shard_id, src, atype, off, len);
        }
    }

    void assoc_range_local(
        std::vector<ThriftAssoc>& _return,
        int32_t shardId,
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len)
    {
        local_shards_[shardId / total_num_hosts_]
            .assoc_range(_return, src, atype, off, len);
    }

    int64_t assoc_count(int64_t src, int64_t atype) {
        int shard_id = src % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            return local_shards_[shard_id / total_num_hosts_]
                .assoc_count(src, atype);
        } else {
            return aggregators_.at(host_id).assoc_count_local(
                shard_id, src, atype);
        }
    }

    int64_t assoc_count_local(int32_t shardId, int64_t src, int64_t atype) {
        return local_shards_[shardId / total_num_hosts_]
            .assoc_count(src, atype);
    }

    void assoc_get(
        std::vector<ThriftAssoc>& _return,
        const int64_t src,
        const int64_t atype,
        const std::set<int64_t>& dstIdSet,
        const int64_t tLow,
        const int64_t tHigh)
    {
        COND_LOG_E("in agg. assoc_get()\n");
        int shard_id = src % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            COND_LOG_E("sending to shard %d\n", shard_id);
            local_shards_[shard_id / total_num_hosts_]
                .assoc_get(_return, src, atype, dstIdSet, tLow, tHigh);
            COND_LOG_E("done\n");
        } else {
            aggregators_.at(host_id).assoc_get_local(
                _return, shard_id, src, atype, dstIdSet, tLow, tHigh);
        }
    }

    void assoc_get_local(
        std::vector<ThriftAssoc>& _return,
        const int32_t shardId,
        const int64_t src,
        const int64_t atype,
        const std::set<int64_t>& dstIdSet,
        const int64_t tLow,
        const int64_t tHigh)
    {
        local_shards_[shardId / total_num_hosts_]
            .assoc_get(_return, src, atype, dstIdSet, tLow, tHigh);
    }

    void obj_get(std::vector<std::string>& _return, const int64_t nodeId) {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            local_shards_[shard_id / total_num_hosts_]
                .obj_get(_return, global_to_local_node_id(nodeId, shard_id));
        } else {
            aggregators_.at(host_id).obj_get_local(_return, shard_id, nodeId);
        }
    }

    void obj_get_local(
        std::vector<std::string>& _return,
        const int32_t shardId,
        const int64_t nodeId)
    {
        local_shards_[shardId / total_num_hosts_]
            .obj_get(_return, global_to_local_node_id(nodeId, shardId));
    }

    void assoc_time_range(
        std::vector<ThriftAssoc>& _return,
        const int64_t src,
        const int64_t atype,
        const int64_t tLow,
        const int64_t tHigh,
        const int32_t limit)
    {
        int shard_id = src % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            local_shards_[shard_id / total_num_hosts_]
                .assoc_time_range(_return, src, atype, tLow, tHigh, limit);
        } else {
            aggregators_.at(host_id).assoc_time_range_local(
                _return, shard_id, src, atype, tLow, tHigh, limit);
        }
    }

    void assoc_time_range_local(
        std::vector<ThriftAssoc>& _return,
        const int32_t shardId,
        const int64_t src,
        const int64_t atype,
        const int64_t tLow,
        const int64_t tHigh,
        const int32_t limit)
    {
        local_shards_[shardId / total_num_hosts_]
            .assoc_time_range(_return, src, atype, tLow, tHigh, limit);
    }

private:

    // globalKey = localKey * numShards + shardId
    // localKey = (globalKey - shardId) / numShards
    inline int64_t global_to_local_node_id(
        int64_t global_node_id, int shard_id)
    {
        return (global_node_id - shard_id) / total_num_shards_;
    }

    const int total_num_shards_; // total # of logical shards
    const int local_num_shards_;

    // id of this physical node
    const int local_host_id_;
    // all aggregators in the cluster, hostnames used for opening sockets
    std::vector<std::string> hostnames_;
    const int total_num_hosts_;

    std::vector<GraphQueryServiceClient> local_shards_;

    // Maps host id to aggregator handle.  Does not contain self.
    std::unordered_map<int, GraphQueryAggregatorServiceClient> aggregators_;
};

// Dummy factory that just delegates fields.
class ProcessorFactory : public TProcessorFactory {
public:
    ProcessorFactory(
        int total_num_shards,
        int local_num_shards,
        int local_host_id,
        const std::vector<std::string>& hostnames)
    : total_num_shards_(total_num_shards),
      local_num_shards_(local_num_shards),
      local_host_id_(local_host_id),
      hostnames_(hostnames)
    { }

    boost::shared_ptr<TProcessor> getProcessor(const TConnectionInfo&) {
        boost::shared_ptr<GraphQueryAggregatorServiceHandler> handler(
            new GraphQueryAggregatorServiceHandler(total_num_shards_,
                local_num_shards_, local_host_id_, hostnames_));
        boost::shared_ptr<TProcessor> handlerProcessor(
            new GraphQueryAggregatorServiceProcessor(handler));
        return handlerProcessor;
    }
private:
    int total_num_shards_;
    int local_num_shards_;
    int local_host_id_;
    const std::vector<std::string>& hostnames_;
};

void print_usage(char *exec) {
    LOG_E(
        "Usage: %s [-t total_num_shards] [-s local_num_shards] "
        "[-h hostsfile] [-i local_host_id]\n",
        exec);
}

int main(int argc, char **argv) {
    if (argc < 2 || argc > 10) {
        print_usage(argv[0]);
        return -1;
    }

    int c, total_num_shards, local_num_shards, local_host_id;
    std::string hostsfile;
    while ((c = getopt(argc, argv, "t:s:i:h:")) != -1) {
        switch(c) {
        case 't':
            total_num_shards = atoi(optarg);
            break;
        case 's':
            local_num_shards = atoi(optarg);
            break;
        case 'i':
            local_host_id = atoi(optarg);
            break;
        case 'h':
            hostsfile = optarg;
            break;
        default:
            total_num_shards = local_num_shards = 1;
            local_host_id = 0;
            hostsfile = "./conf/hosts";
        }
    }

    std::ifstream hosts(hostsfile);
    std::string host;
    std::vector<std::string> hostnames;
    while (std::getline(hosts, host)) {
        hostnames.push_back(host);
    }

    int port = QUERY_HANDLER_PORT;
    try {
        shared_ptr<ProcessorFactory> processor_factory(
            new ProcessorFactory(
                total_num_shards,
                local_num_shards,
                local_host_id,
                hostnames));
        shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
        shared_ptr<TTransportFactory> transport_factory(
            new TBufferedTransportFactory());
        shared_ptr<TProtocolFactory> protocol_factory(
            new TBinaryProtocolFactory());

        // Note: 1st arg being a processor factory is essential in supporting
        // multiple clients (e.g. in throughput benchmarks).
        TThreadedServer server(
            processor_factory,
            server_transport,
            transport_factory,
            protocol_factory);

        server.serve();
    } catch (std::exception& e) {
        LOG_E("Exception at GraphQueryAggregator:main(): %s\n", e.what());
    }
    return 0;
}
