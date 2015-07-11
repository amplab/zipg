#include "thrift/GraphQueryAggregatorService.h"

#include <fstream>
#include <unordered_map>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>

#include "rpc/ports.h"
#include "succinct-graph/utils.h"
#include <thrift/GraphQueryService.h>

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

    void init() {
        for (auto shard : local_shards_)
            shard.send_init();
        for (auto shard : local_shards_)
            shard.recv_init();
    }

    void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        COND_LOG_E(
            "Received: get_neighbors(%lld), route to shard %d on host %d\n",
            nodeId, shard_id, host_id);
        if (host_id == local_host_id_) {
            local_shards_.at(shard_id / total_num_hosts_)
                .get_neighbors(_return, nodeId);
        } else {
            assert(false && "routing not implemented");
        }
    }

    void get_neighbors_atype(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        int shard_id = nodeId % total_num_shards_;
        int host_id = shard_id % total_num_hosts_;
        if (host_id == local_host_id_) {
            local_shards_.at(shard_id / total_num_hosts_)
                .get_neighbors_atype(_return, nodeId, atype);
        } else {
            assert(false && "routing not implemented");
        }
    }

    void get_neighbors_attr(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int32_t attrId,
        const std::string& attrKey)
    {
        std::vector<int64_t> nhbrs;
        get_neighbors(nhbrs, nodeId);

        std::unordered_map<int, std::vector<int64_t>> splits_by_keys;
        int shard_id, host_id;
        for (int64_t nhbr_id : nhbrs) {
            shard_id = nhbr_id % total_num_shards_;
            // globalKey = localKey * numShards + shardId
            // localKey = (globalKey - shardId) / numShards
            splits_by_keys[shard_id].push_back(
                (nhbr_id - shard_id) / total_num_shards_);

            host_id = shard_id % total_num_hosts_;
            assert(host_id == local_host_id_);
        }

        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            local_shards_.at(it->first).send_filter_nodes(
                it->second, attrId, attrKey);
        }

        _return.clear();
        std::vector<int64_t> shard_result;
        for (auto it = splits_by_keys.begin(); it != splits_by_keys.end(); ++it)
        {
            local_shards_.at(it->first).recv_filter_nodes(shard_result);
            for (int64_t local_key : shard_result) {
                _return.push_back(local_key * total_num_shards_ + it->first);
            }
        }
    }

    void get_nodes(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        // TODO: aggregator-to-aggregator routing is not yet implemented
        assert(local_num_shards_ == total_num_shards_);

        for (auto shard : local_shards_) {
            shard.send_get_nodes(attrId, attrKey);
        }

        std::set<int64_t> shard_result;
        _return.clear();
        for (auto shard : local_shards_) {
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
        // TODO: aggregator-to-aggregator routing is not yet implemented
        assert(local_num_shards_ == total_num_shards_);

        for (auto shard : local_shards_) {
            shard.send_get_nodes2(attrId1, attrKey1, attrId2, attrKey2);
        }

        std::set<int64_t> shard_result;
        _return.clear();
        for (auto shard : local_shards_) {
            shard.recv_get_nodes2(shard_result);
            _return.insert(shard_result.begin(), shard_result.end());
        }
    }

private:
    const int total_num_shards_; // total # of logical shards
    const int local_num_shards_;

    // id of this physical node
    const int local_host_id_;
    // all aggregators in the cluster, hostnames used for opening sockets
    std::vector<std::string> hostnames_;
    const int total_num_hosts_;

    std::vector<GraphQueryServiceClient> local_shards_;
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
        shared_ptr<GraphQueryAggregatorServiceHandler> handler(
            new GraphQueryAggregatorServiceHandler(
                total_num_shards,
                local_num_shards,
                local_host_id,
                hostnames));

        shared_ptr<TProcessor> processor(
            new GraphQueryAggregatorServiceProcessor(handler));
        shared_ptr<TServerTransport> server_transport(new TServerSocket(port));
        shared_ptr<TTransportFactory> transport_factory(
            new TBufferedTransportFactory());
        shared_ptr<TProtocolFactory> protocol_factory(
            new TBinaryProtocolFactory());
        TThreadedServer server(
            processor,
            server_transport,
            transport_factory,
            protocol_factory);

        server.serve();
    } catch (std::exception& e) {
        LOG_E("Exception at GraphQueryAggregator:main(): %s\n", e.what());
    }
    return 0;
}
