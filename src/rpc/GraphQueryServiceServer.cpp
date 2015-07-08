#include "thrift/GraphQueryService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "succinct-graph/SuccinctGraph.hpp"
#include "rpc/ports.h"

#include <sys/stat.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

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
        int total_num_shards)
    : shard_id_(shard_id),
      total_num_shards_(total_num_shards),
      node_file_(node_file),
      edge_file_(edge_file),
      node_table_empty_(file_exists(node_file)),
      edge_table_empty_(file_exists(edge_file)),
      construct_(construct),
      graph_(new SuccinctGraph("")) // just no-op object alloc
    {
        graph_->set_npa_sampling_rate(npa_sampling_rate);
        graph_->set_sa_sampling_rate(sa_sampling_rate);
        graph_->set_isa_sampling_rate(isa_sampling_rate);
    }

    // Loads or constructs graph shards.
    void init() {
        if (construct_) {
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
        fprintf(stderr, "Initialization at this shard: done\n");
    }

    // In principle, nodeId should be in this shard's edge table.
    void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
        // Your implementation goes here
        printf("get_neighbors\n");

        assert(nodeId % total_num_shards_ == shard_id_);
        if (edge_table_empty_) {
            _return.clear();
            return;
        }
        graph_->get_neighbors(_return, nodeId);
    }

    void get_neighbors_atype(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int64_t atype)
    {
        // Your implementation goes here
        printf("get_neighbors_atype\n");

        assert(nodeId % total_num_shards_ == shard_id_);
        if (edge_table_empty_) {
            _return.clear();
            return;
        }
        graph_->get_neighbors(_return, nodeId, atype);
    }

    void get_neighbors_attr(
        std::vector<int64_t> & _return,
        const int64_t nodeId,
        const int32_t attrId,
        const std::string& attrKey)
    {
        // Your implementation goes here
        printf("get_neighbors_attr\n");

        assert(false &&
            "Algorithm for get_nhbr(n, attr) should not contact shards");
    }

    void get_nodes(
        std::set<int64_t> & _return,
        const int32_t attrId,
        const std::string& attrKey)
    {
        // Your implementation goes here
        printf("get_nodes\n");

        if (node_table_empty_) {
            _return.clear();
            return;
        }
        graph_->get_nodes(_return, attrId, attrKey);
    }

    void get_nodes2(
        std::set<int64_t> & _return,
        const int32_t attrId1,
        const std::string& attrKey1,
        const int32_t attrId2,
        const std::string& attrKey2)
    {
        // Your implementation goes here
        printf("get_nodes2\n");

        if (node_table_empty_) {
            _return.clear();
            return;
        }
        graph_->get_nodes(_return, attrId1, attrKey1, attrId2, attrKey2);
    }

private:

    inline bool file_exists(const std::string& pathname) {
        struct stat buffer;
        return (stat(pathname.c_str(), &buffer) == 0);
    }

    const int shard_id_;
    const int total_num_shards_;

    const std::string node_file_;
    const std::string edge_file_;
    const bool node_table_empty_;
    const bool edge_table_empty_;
    const bool construct_;
    const shared_ptr<SuccinctGraph> graph_;
};

int main(int argc, char **argv) {
    if (argc < 2 || argc > 16)
        return -1;
    fprintf(stderr, "Command line: ");
    for (int i = 0; i < argc; i++) fprintf(stderr, "%s ", argv[i]);
    fprintf(stderr, "\n");

    int c;
    uint32_t mode, port, sa_sampling_rate, isa_sampling_rate, npa_sampling_rate;
    int shard_id, total_num_shards;
    while ((c = getopt(argc, argv, "m:p:s:i:n:t:d:")) != -1) {
        switch(c) {
        case 'm':
            mode = atoi(optarg); // 0 for construct, 1 for load
            assert(mode == 0 && "Loading constructed shards not supported atm");
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
        default:
            shard_id = 0;
            total_num_shards = 1;
            mode = 0; // construct
            port = QUERY_SERVER_PORT;
            // default Succinct Graph level 0
            isa_sampling_rate = 64;
            sa_sampling_rate = 32;
            npa_sampling_rate = 128;
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
                total_num_shards));

        shared_ptr<TProcessor> processor(
            new GraphQueryServiceProcessor(handler));
        shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
        shared_ptr<TTransportFactory> transportFactory(
            new TBufferedTransportFactory());
        shared_ptr<TProtocolFactory> protocolFactory(
            new TBinaryProtocolFactory());
        // TODO: simple server vs. threaded server?
        TSimpleServer server(
            processor, serverTransport, transportFactory, protocolFactory);

        server.serve();
    } catch (std::exception& e) {
        fprintf(stderr,
                "Exception at GraphQueryServiceServer:main(): %s\n",
                e.what());
    }
    return 0;
}
