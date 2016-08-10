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
  std::vector<int64_t> offsets;
  std::vector<DeletedEdges::edge_record_id> edge_record_ids;
  int64_t src, atype, count, psize_width;
  int64_t cur_offset = 0;
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

    DeletedEdges::edge_record_id rec = { src, atype };
    edge_record_ids.push_back(rec);
    offsets.push_back(cur_offset);
    cur_offset += count;
  }
  in.close();

  std::ofstream out(output);
  std::cout << "Total number of edges = " << cur_offset << "\n";

  DeletedEdges del(edge_record_ids, offsets, cur_offset);

  std::cout << "Created deleted edges with " << del.GetNumEdges()
            << " edges and " << del.GetNumRecords() << " edge records.\n";
  del.Serialize(out);

  out.close();

  return 0;
}

