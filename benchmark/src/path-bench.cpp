#include <cassert>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <initializer_list>
#include <string>
#include <sstream>
#include <vector>

#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"
#include "utils.h"
#include "GraphBenchmark.hpp"

using boost::shared_ptr;

template<typename Out>
void split(const std::string &s, char delim, Out result) {
  std::stringstream ss;
  ss.str(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    *(result++) = item;
  }
}

std::set<std::string> split(const std::string &s, char delim) {
  std::set<std::string> elems;
  split(s, delim, std::inserter(elems, elems.end()));
  return elems;
}

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
    int sum = 0;
    LOG_E("Starting warmup...\n");
    for (size_t i = 0; i < queries_.size(); i++) {
      // Run query
      for (auto query : queries_.at(i)) {
        if (query.back() == '*')
          continue;
        sum += aggregator_->count_regular_path_query(query);
      }
    }
    LOG_E("Completed warmup, sum = %lld\n", sum);

    // Measure
    std::ofstream out(output_file);
    LOG_E("Starting measure...\n");
    for (size_t i = 0; i < queries_.size(); i++) {
      time_t start, tot;
      start = get_timestamp();
      // Run query
      int64_t cnt = 0;
      bool recursive = false;
      for (auto query : queries_.at(i)) {
        if (query.back() == '*') {
          recursive = true;
          break;
        } else {
          fprintf(stderr, "Query: %s\n", query.c_str());
        }
        cnt += aggregator_->count_regular_path_query(query);
      }

      tot = get_timestamp() - start;
      if (recursive)
        out << i << "\t" << -1 << "\t" << -1 << "\n";
      else
        out << i << "\t" << cnt << "\t" << tot << "\n";
    }
    LOG_E("Finished measure.\n");
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
      queries_.push_back(split(exp, '\t'));
    }
    fprintf(stderr, "Loaded %zu queries.\n", queries_.size());
  }

  std::vector<std::set<std::string>> queries_;

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
