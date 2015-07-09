#include "thrift/GraphQueryAggregatorService.h"

#include <iostream>
#include <fstream>
#include <unistd.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "rpc/ports.h"
#include "succinct-graph/utils.h"

using boost::shared_ptr;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

class ShardedGraphBenchmark {
public:

    ShardedGraphBenchmark(int total_num_shards)
    : total_num_shards_(total_num_shards)
    { }

    void init() {
        try {
            int port = QUERY_HANDLER_PORT;
            fprintf(stderr, "Connecting to server...\n");
            shared_ptr<TSocket> socket(new TSocket("localhost", port));
            shared_ptr<TTransport> transport(
                    new TBufferedTransport(socket));
            shared_ptr<TProtocol> protocol(
                    new TBinaryProtocol(transport));
            aggregator = shared_ptr<GraphQueryAggregatorServiceClient>(
                new GraphQueryAggregatorServiceClient(protocol));
            transport->open();
            LOG_E("Connected to aggregator!\n");

            int ret = aggregator->connect_to_local_shards();
            LOG_E("Aggregator connected to local shards, ret = %d\n", ret);

            aggregator->init();
            LOG_E("Done init all shards\n");

            std::vector<int64_t> nhbrs;
            aggregator->get_neighbors(nhbrs, 0);
            print_vector("nhbrs of 0: ", nhbrs);
            aggregator->get_neighbors(nhbrs, 1);
            print_vector("nhbrs of 1: ", nhbrs);
            aggregator->get_neighbors(nhbrs, 6);
            print_vector("nhbrs of 6: ", nhbrs);

        } catch (std::exception& e) {
            LOG_E("Exception in benchmark client: %s\n", e.what());
        }
    }

    void bench_get_neighbor_latency() {
    }

private:
    const int total_num_shards_;
    shared_ptr<GraphQueryAggregatorServiceClient> aggregator;

    void print_vector(const std::string& msg, const std::vector<int64_t>& vec) {
        printf("%s[", msg.c_str());
        for (auto it = vec.begin(); it != vec.end(); ++it)
            printf(" %lld", *it);
        printf(" ]\n");
    }
};

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t num-threads] bench-type\n", exec);
}

int main(int argc, char **argv) {
//    if (argc < 2 || argc > 12) {
//        print_usage(argv[0]);
//        return -1;
//    }

    int c;
    int total_num_shards, total_num_hosts;

    while ((c = getopt(argc, argv, "q:t:s:k:l:")) != -1) {
        switch(c) {
        case 's':
            total_num_shards = atoi(optarg);
            break;
        case 'q':
//            queryfile = std::string(optarg);
            break;
        default:
            total_num_shards = 1;
            total_num_hosts = 1; // TODO: make configurable

//            queryfile = "";
        }
    }

    assert(total_num_shards % total_num_hosts == 0);

//    if (optind == argc) {
//        print_usage(argv[0]);
//        return -1;
//    }
//    std::string benchmark_type = std::string(argv[optind]);

    ShardedGraphBenchmark bench(total_num_shards);
    bench.init();

    return 0;
}
