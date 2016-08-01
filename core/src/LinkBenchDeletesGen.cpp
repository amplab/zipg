#include <fstream>
#include <sstream>
#include "SuccinctGraph.hpp"
#include "utils.h"

// Used in edge table layout only.
const char SuccinctGraph::NODE_ID_DELIM = '\x02';
const char SuccinctGraph::ATYPE_DELIM = '\x03';
const char SuccinctGraph::TIMESTAMP_WIDTH_DELIM = '\x04';  // delim right before timestamp width
const char SuccinctGraph::EDGE_WIDTH_DELIM = '\x05';  // delim right before edge width
const char SuccinctGraph::METADATA_DELIM = '\x06';  // delim after all these header metadata

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s [input]\n", argv[0]);
    return -1;
  }

  std::string input = std::string(argv[1]);
  // std::string output = input + ".deletes";

  std::ifstream in(input);
  // std::ofstream out(output);
  std::string buf; // Buffer
  while (std::getline(in, buf, SuccinctGraph::NODE_ID_DELIM) && !in.eof()) {
    int64_t src, atype, count;
    std::getline(in, buf, SuccinctGraph::ATYPE_DELIM);
    src = std::stoll(buf);
    std::cout << src << "\t";
    std::getline(in, buf, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);
    atype = std::stoll(buf);
    std::cout << atype << "\t";
    std::getline(in, buf, SuccinctGraph::EDGE_WIDTH_DELIM);
    count = std::stoll(buf.substr(4));
    std::cout << count << std::endl;
  }

  return 0;
}

