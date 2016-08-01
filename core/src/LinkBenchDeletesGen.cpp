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

// Used in node table layout only.
// *****Note that it is important the delim is not in DELIMITERS.*****
const char SuccinctGraph::NODE_TABLE_HEADER_DELIM = '\x1F';
const std::vector<unsigned char> SuccinctGraph::DELIMITERS = {
    // 20 non-ASCII delims (ord >= 128)
    128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142,
    143, 144, 145, 146, 147,
    // ASCII delims that are not alphanumeric (unlikely to be used), ord < 128
    '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x0C', '\x0D',
    '\x0E', '\x0F', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16',
    '\x17', '\x18', '\x19', '\x1A', '\x1B', '\x1C', '\x1D', '\x1E' };

std::string pad_timestamp_width(int32_t x) {
  if (x < 10)
    return '0' + std::to_string(x);
  return std::to_string(x);
}

std::string pad_dst_id_width(int32_t x) {
  if (x < 10)
    return '0' + std::to_string(x);
  return std::to_string(x);
}

std::string encode_timestamp(int64_t timestamp, int32_t padded_width) {
  char* res = new char[padded_width + 1];
  sprintf(res, "%0*lld", padded_width, timestamp);
  return std::string(res);
}

int64_t decode_timestamp(const std::string& encoded) {
  return std::stoll(encoded);
}

std::string encode_node_id(int64_t node_id, int32_t padded_width) {
  char res[padded_width + 1];
  sprintf(res, "%0*lld", padded_width, node_id);
  return std::string(res);
}

int64_t decode_node_id(const std::string& encoded) {
  return std::stol(encoded);
}

int32_t num_digits(int64_t number) {
  if (number == 0)
    return 1;
  int32_t digits = 0;
  while (number != 0) {
    number /= 10;
    ++digits;
  }
  return digits;
}

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
  while (!in.eof()) {
    int64_t src, atype, count;
    std::getline(in, buf, SuccinctGraph::NODE_ID_DELIM);
    std::getline(in, buf, SuccinctGraph::ATYPE_DELIM);
    src = std::stoll(buf);
    std::getline(in, buf, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);
    atype = std::stoll(buf);
    std::getline(in, buf, SuccinctGraph::EDGE_WIDTH_DELIM);
    count = std::stoll(buf.substr(4));
    std::cout << src << "\t" << atype << "\t" << count << std::endl;
  }

  return 0;
}

