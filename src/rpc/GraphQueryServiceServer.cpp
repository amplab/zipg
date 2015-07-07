#include "thrift/GraphQueryService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

class GraphQueryServiceHandler : virtual public GraphQueryServiceIf {
 public:
  GraphQueryServiceHandler() {
    // Your initialization goes here
  }

  void get_neighbors(std::vector<int64_t> & _return, const int64_t nodeId) {
    // Your implementation goes here
    printf("get_neighbors\n");
  }

  void get_neighbors_atype(std::vector<int64_t> & _return, const int64_t nodeId, const int64_t atype) {
    // Your implementation goes here
    printf("get_neighbors_atype\n");
  }

  void get_neighbors_attr(std::vector<int64_t> & _return, const int64_t nodeId, const int32_t attrId, const std::string& attrKey) {
    // Your implementation goes here
    printf("get_neighbors_attr\n");
  }

  void get_nodes(std::vector<int64_t> & _return, const int32_t attrId, const std::string& attrKey) {
    // Your implementation goes here
    printf("get_nodes\n");
  }

  void get_nodes2(std::vector<int64_t> & _return, const int32_t attrId1, const std::string& attrKey1, const int32_t attrId2, const std::string& attrKey2) {
    // Your implementation goes here
    printf("get_nodes2\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<GraphQueryServiceHandler> handler(new GraphQueryServiceHandler());
  shared_ptr<TProcessor> processor(new GraphQueryServiceProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

