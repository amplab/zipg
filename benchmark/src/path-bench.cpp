#include <cassert>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <initializer_list>

#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"
#include "utils.h"
#include "GraphBenchmark.hpp"

using boost::shared_ptr;

class PathBench {
 public:
  PathBench(const std::string& query_file) {
    load_queries(query_file);

    try {
      LOG_E("Connecting to localhost...\n");
      shared_ptr<TSocket> socket(new TSocket("localhost", QUERY_HANDLER_PORT));
      transport_ = shared_ptr<TTransport>(new TBufferedTransport(socket));
      shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport_));
      aggregator_ = shared_ptr<GraphQueryAggregatorServiceClient>(
          new GraphQueryAggregatorServiceClient(protocol));
      transport_->open();
      LOG_E("Connected to localhost!\n");

      int ret = aggregator_->init();
      LOG_E("Initialization = %d\n", ret);
    } catch (std::exception& e) {
      LOG_E("Exception in benchmark client: %s\n", e.what());
    }
  }

  void run() {
    std::string output_file = "path_query_latency.txt";

    // Warmup
    for (size_t i = 0; i < queries_.size(); i++) {
      // Run query
      RPQCtx ctx;
      aggregator_->regular_path_query(ctx, queries_.at(i));
    }

    // Measure
    std::ofstream out(output_file);
    for (size_t i = 0; i < queries_.size(); i++) {
      time_t start, tot;
      start = get_timestamp();
      // Run query
      RPQCtx ctx;
      aggregator_->regular_path_query(ctx, queries_.at(i));
      tot = get_timestamp() - start;
      out << i << "\t" << ctx.endpoints.size() << "\t" << tot << "\n";
    }
  }

 private:
  void load_queries(const std::string& query_path) {
    fprintf(stderr, "Loading queries...\n");
    std::ifstream in(query_path);
    if (!in.is_open()) {
      fprintf(stderr, "Could not open query file %s\n", query_path.c_str());
      exit(-1);
    }

    std::string exp;
    while (std::getline(in, exp)) {
      queries_.push_back(exp);
    }
    fprintf(stderr, "Loaded %zu queries.\n", queries_.size());
  }

  std::vector<std::string> queries_;

  shared_ptr<GraphQueryAggregatorServiceClient> aggregator_;
  shared_ptr<TTransport> transport_;
};

void print_usage(char *exec) {
  fprintf(stderr, "Usage: %s [query-file]\n", exec);
}

int main(int argc, char **argv) {
  if (argc != 2) {
    print_usage(argv[0]);
    return -1;
  }

  std::string query_file = std::string(argv[1]);
  PathBench bench(query_file);
  bench.run();
}
