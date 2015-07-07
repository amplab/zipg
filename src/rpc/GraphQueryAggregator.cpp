#include "thrift/GraphQueryAggregatorService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include <thrift/transport/TSocket.h>
#include <thrift/GraphQueryService.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

class GraphQueryAggregatorServiceHandler : virtual public GraphQueryAggregatorServiceIf {
 public:
  GraphQueryAggregatorServiceHandler() {
    // Your initialization goes here
  }

  int32_t connect_to_local_servers() {
    // Your implementation goes here
    printf("connect_to_local_servers\n");
  }

  void get_neighbors_local(std::vector<int64_t> & _return, const int64_t nodeId) {
    // Your implementation goes here
    printf("get_neighbors_local\n");
  }

  void get_neighbors_atype_local(std::vector<int64_t> & _return, const int64_t nodeId, const int64_t atype) {
    // Your implementation goes here
    printf("get_neighbors_atype_local\n");
  }

  void get_neighbors_attr_local(std::vector<int64_t> & _return, const int64_t nodeId, const int32_t attrId, const std::string& attrKey) {
    // Your implementation goes here
    printf("get_neighbors_attr_local\n");
  }

  void get_nodes_local(std::vector<int64_t> & _return, const int32_t attrId, const std::string& attrKey) {
    // Your implementation goes here
    printf("get_nodes_local\n");
  }

  void get_nodes2_local(std::vector<int64_t> & _return, const int32_t attrId1, const std::string& attrKey1, const int32_t attrId2, const std::string& attrKey2) {
    // Your implementation goes here
    printf("get_nodes2_local\n");
  }

};

int main(int argc, char **argv) {

    shared_ptr<TSocket> socket(new TSocket("localhost", 9090));
    shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    GraphQueryServiceClient qsclient(protocol);
    transport->open();
    fprintf(stderr, "Connected!\n");
    std::vector<int64_t> nhbrs;
    qsclient.get_neighbors(nhbrs, 1);
    fprintf(stderr, "Pushed!\n");

//  int port = 9090;
//  shared_ptr<GraphQueryAggregatorServiceHandler> handler(new GraphQueryAggregatorServiceHandler());
//  shared_ptr<TProcessor> processor(new GraphQueryAggregatorServiceProcessor(handler));
//  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
//  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
//  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
//
//  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
//  server.serve();
  return 0;
}

