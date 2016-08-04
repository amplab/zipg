#include <fstream>
#include <sstream>
#include <tuple>
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
  std::string output = input + ".deletes";

  std::ifstream in(input);

  std::string buf;  // Buffer
  std::vector<int32_t> psizes;
  std::vector<std::tuple<int64_t, int64_t, int64_t>> counts;
  int64_t src, atype, count, psize_width;
  while (std::getline(in, buf, SuccinctGraph::NODE_ID_DELIM) && !in.eof()) {
    std::getline(in, buf, SuccinctGraph::ATYPE_DELIM);
    src = std::stoll(buf);

    std::getline(in, buf, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);
    atype = std::stoll(buf);

    std::getline(in, buf, SuccinctGraph::EDGE_WIDTH_DELIM);
    count = std::stoll(buf.substr(4));

    std::getline(in, buf, SuccinctGraph::METADATA_DELIM);
    psize_width = std::stoi(buf);
    assert(psize_width == 3);

    counts.push_back(std::make_tuple(src, atype, count));
  }
  in.close();

  std::ofstream out(output);
  size_t num_entries = counts.size();
  out.write(reinterpret_cast<const char*>(&num_entries), sizeof(size_t));
  for (std::tuple tup : counts) {
    int64_t src, atype, count;
    std::tie(src, atype, count) = tup;
    // Write deletes bitmap to file
    out.write(reinterpret_cast<const char *>(&src), sizeof(int64_t));
    out.write(reinterpret_cast<const char *>(&atype), sizeof(int64_t));
    bitmap::Bitmap invalid_edges(count);
    invalid_edges.Serialize(out, count);
  }
  out.close();

  return 0;
}

