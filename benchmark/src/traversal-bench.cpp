#include <cassert>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <initializer_list>
#include <string>
#include <sstream>
#include <vector>
#include <condition_variable>
#include <thread>
#include <chrono>

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

class TraversalBench {
 public:
  static const uint64_t TIMEOUT_SECS = 600;
  static const size_t NUM_IDS = 1;

  TraversalBench(const std::string& result_file) {
    output_path_ = result_file;

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

  void bfs_bench() {
    // Run once to see if it times out
    LOG_E("Warming up...\n");
    size_t sum = 0;
    for (int64_t i = 0; i < NUM_IDS; i++) {

      // Run query
      sum += run_bfs(i);
    }
    fprintf(stderr, "Warmup complete, sum = %zu\n", sum);

    // Measure
    LOG_E("Starting measure phase...\n");
    std::ofstream out(output_path_);
    for (int64_t i = 0; i < NUM_IDS; i++) {
      time_t start, tot;
      start = get_timestamp();
      // Run query
      int64_t cnt = run_bfs(i);
      tot = get_timestamp() - start;
      out << cnt << "\t" << tot << "\n";
    }
    out.close();
    LOG_E("Finished measure.\n");
  }

  void dfs_bench() {
    // Run once to see if it times out
    LOG_E("Warming up...\n");
    size_t sum = 0;
    for (int64_t i = 0; i < NUM_IDS; i++) {

      // Run query
      sum += run_dfs(i);
    }
    fprintf(stderr, "Warmup complete, sum = %zu\n", sum);

    // Measure
    LOG_E("Starting measure phase...\n");
    std::ofstream out(output_path_);
    for (int64_t i = 0; i < NUM_IDS; i++) {
      time_t start, tot;
      start = get_timestamp();
      // Run query
      int64_t cnt = run_dfs(i);
      tot = get_timestamp() - start;
      out << cnt << "\t" << tot << "\n";
    }
    out.close();
    LOG_E("Finished measure.\n");
  }

 private:
  size_t run_bfs(int64_t start_id) {
    std::vector<int64_t> res;
    aggregator_->BFS(res, start_id);
    return res.size();
  }

  size_t run_dfs(int64_t start_id) {
    std::vector<int64_t> res;
    aggregator_->DFS(res, start_id);
    return res.size();
  }

  std::string query_str_;
  std::set<std::string> query_;
  std::string output_path_;

  shared_ptr<GraphQueryAggregatorServiceClient> aggregator_;
  shared_ptr<TTransport> transport_;
};

void print_usage(char *exec) {
  fprintf(stderr, "Usage: %s [traversal-type] [output-file]\n", exec);
}

int main(int argc, char **argv) {
  if (argc != 3) {
    print_usage(argv[0]);
    return -1;
  }
  std::string traversal_type = std::string(argv[1]);
  std::string result_file = std::string(argv[2]);
  TraversalBench bench(result_file);

  if (traversal_type == "bfs") {
    bench.bfs_bench();
  } else if (traversal_type == "dfs") {
    bench.dfs_bench();
  } else {
    fprintf(stderr, "Invalid traversal type %s\n", traversal_type.c_str());
  }

  return 0;
}
