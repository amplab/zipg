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
  std::string output = input + ".deletes";

  std::ifstream in(input);
  std::ofstream out(output);
  std::string buf;  // Buffer
  std::vector<int32_t> psizes;
  int64_t src, atype, count, ts_width, dst_width, psize_width;
  while (true) {
    std::getline(in, buf, SuccinctGraph::NODE_ID_DELIM);
    if (buf != "")
      assert(buf == "\n");

    if (in.eof())
      break;

    std::getline(in, buf, SuccinctGraph::ATYPE_DELIM);
    src = std::stoll(buf);

    std::getline(in, buf, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);
    atype = std::stoll(buf);

    std::getline(in, buf, SuccinctGraph::EDGE_WIDTH_DELIM);
    ts_width = std::stoll(buf.substr(0, 2));
    dst_width = std::stoll(buf.substr(2, 2));
    count = std::stoll(buf.substr(4));

    std::getline(in, buf, SuccinctGraph::METADATA_DELIM);
    psize_width = std::stoi(buf);
    assert(psize_width == 3);

    char* ts_buf = new char[ts_width * count];
    in.read(ts_buf, sizeof(char) * ts_width * count);
    delete[] ts_buf;

    char* dst_buf = new char[dst_width * count];
    in.read(dst_buf, sizeof(char) * dst_width * count);
    delete[] dst_buf;

    char* psizebuf = new char[psize_width];
    psizes.clear();
    for (int64_t i = 0; i < count; i++) {
      in.read(psizebuf, psize_width);
      int32_t psize = std::atoi(psizebuf);
      psizes.push_back(psize);
      char* prop = new char[psize];
      in.read(prop, psize);
      delete[] prop;
    }
    delete[] psizebuf;

    // Write deletes bitmap to file
    out.write(reinterpret_cast<const char *>(&src), sizeof(int64_t));
    out.write(reinterpret_cast<const char *>(&atype), sizeof(int64_t));
    bitmap::Bitmap invalid_edges(count);
    invalid_edges.Serialize(out, count);
  }

  in.close();
  out.close();

  return 0;
}

