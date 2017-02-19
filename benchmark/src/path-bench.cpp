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

class PathBench {
 public:
  static const uint64_t TIMEOUT_SECS = 600;
  static const size_t NUM_RUNS = 1;

  PathBench(const std::string& query_file) {
    load_query(query_file);
    output_path_ = query_file + ".results";

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
    // Run once to see if it times out
    LOG_E("Testing query for timeout...\n");
    std::ofstream out(output_path_);
    if (run_query_timeout() == -1) {
      fprintf(stderr, "Timeout! Query %s did not finish.\n",
              query_str_.c_str());
      out << "DNF";
      out.close();
      return;
    }

    // Measure
    LOG_E("Starting benchmark...\n");
    for (size_t i = 0; i < NUM_RUNS; i++) {
      time_t start, tot;
      start = get_timestamp();
      // Run query
      int64_t cnt = run_query();
      tot = get_timestamp() - start;
      out << cnt << "\t" << tot << "\n";
    }
    out.close();
    LOG_E("Finished measure.\n");
  }

 private:
  int64_t run_query_timeout() {
    std::mutex m;
    std::condition_variable cv;
    int ret = 0;

    std::thread t([&m, &cv, &ret, this]()
    {
      for (auto q : query_) {
        ret += aggregator_->count_regular_path_query(q);
      }
      cv.notify_one();
    });

    t.detach();

    {
      std::unique_lock<std::mutex> l(m);
      if (cv.wait_for(l, std::chrono::seconds(TIMEOUT_SECS))
          == std::cv_status::timeout)
        return -1;
    }

    return ret;
  }

  int64_t run_query() {
    int64_t ret;
    for (auto q : query_) {
      ret += aggregator_->count_regular_path_query(q);
    }
    return ret;
  }

  void load_query(const std::string& query_path) {
    fprintf(stderr, "Loading queries...\n");
    std::ifstream in(query_path);
    if (!in.is_open()) {
      fprintf(stderr, "Could not open query file %s\n", query_path.c_str());
      exit(-1);
    }

    if (std::getline(in, query_str_)) {
      query_ = split(query_str_, '\t');
      fprintf(stderr, "Loaded query.\n");
    } else {
      fprintf(stderr, "Could not load query: is query file empty?\n");
      exit(-1);
    }
  }

  std::string query_str_;
  std::set<std::string> query_;
  std::string output_path_;

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
