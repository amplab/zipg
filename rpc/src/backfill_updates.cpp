#include "GraphQueryAggregatorService.h"
#include "ports.h"
#include "utils.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/PosixThreadFactory.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

// Dummy program to backfill updates.
// Does not take any command line arguments
int main() {
    LOG_E("About to open agg. transport\n");
    boost::shared_ptr<TSocket> socket_(
        new TSocket("localhost", QUERY_HANDLER_PORT));
    boost::shared_ptr<TTransport> transport_(new TBufferedTransport(socket_));
    boost::shared_ptr<TProtocol> protocol_(new TBinaryProtocol(transport_));
    transport_->open();
    LOG_E("connected\n");
    GraphQueryAggregatorServiceClient client_(protocol_);

    client_.init();
    LOG_E("init() done, about to backfill\n");
    time_t t1 = get_timestamp();

    int num = client_.backfill_edge_updates();
    // TODO: node to be implemented
    time_t t2 = get_timestamp();

    LOG_E("backfill_edge_updates() done! filled %d updates in %.1f millis\n",
        num, (static_cast<double>(t2) - t1) / 1e3);


    client_.shutdown();
    transport_->close();
    return 0;
}
