#include <fstream>
#include <sstream>
#include "SuccinctGraph.hpp"
#include "utils.h"

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

void build_assoc_map(
    std::map<SuccinctGraph::AssocListKey, std::vector<SuccinctGraph::Assoc>>& assoc_map,
    const std::string& in) {
  assoc_map.clear();
  std::ifstream edge_file_stream(in);

  std::string line, token;
  SuccinctGraph::AType atype = -1LL;
  SuccinctGraph::Timestamp time = -1LL;
  SuccinctGraph::NodeId src_id = -1LL, dst_id = -1LL;

  while (std::getline(edge_file_stream, line)) {
    std::stringstream ss(line);
    int token_idx = 0;
    while (std::getline(ss, token, ' ')) {
      ++token_idx;
      if (token_idx == 1)
        src_id = std::stoll(token);
      else if (token_idx == 2)
        dst_id = std::stoll(token);
      else if (token_idx == 3)
        atype = std::stoll(token);
      else if (token_idx == 4)
        time = std::stoll(token);
      token.clear();
      if (token_idx == 4)
        break;
    }
    std::getline(ss, token);  // rest of the data is attr

    auto& list = assoc_map[std::make_pair(src_id, atype)];
    list.emplace_back();
    auto& entry = list.back();
    entry.src_id = src_id;
    entry.dst_id = dst_id;
    entry.atype = atype;
    entry.time = time;
    entry.attr = token;
  }

  for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
    std::sort(it->second.begin(), it->second.end(),
              SuccinctGraph::cmp_assoc_by_decreasing_time);
  }
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
    fprintf(stderr, "Usage: %s [input prefix] [output prefix]\n", argv[0]);
    return -1;
  }

  std::string input_prefix = std::string(argv[1]);
  std::string output_prefix = std::string(argv[2]);

  std::string node_file_in = input_prefix + ".node";
  std::string edge_file_in = input_prefix + ".assoc";
  std::string node_file_out = output_prefix + ".node";
  std::string edge_file_out = output_prefix + ".assoc";

  typedef SuccinctGraph::Assoc Assoc;
  typedef SuccinctGraph::AType AType;
  typedef SuccinctGraph::AssocListKey AssocListKey;
  typedef SuccinctGraph::NodeId NodeId;
  typedef SuccinctGraph::Timestamp Timestamp;

  // Format node-file
  {
    std::ifstream node_in(node_file_in);
    std::ofstream node_out(node_file_out);
    std::string node_data;

    while (std::getline(node_in, node_data)) {
      size_t distance = num_digits(node_data.length()) + 1;
      std::string out_line = std::to_string(distance)
          + SuccinctGraph::NODE_TABLE_HEADER_DELIM
          + std::to_string(node_data.length())
          + SuccinctGraph::NODE_TABLE_HEADER_DELIM
          + static_cast<char>(SuccinctGraph::DELIMITERS[0]) + node_data;
      node_out << out_line;
    }
    node_out.close();
  }

  // Format edge-file
  {
    std::map<std::pair<int64_t, int64_t>, std::vector<Assoc>> assoc_map;
    build_assoc_map(assoc_map, edge_file_in);

    std::ofstream edge_out(edge_file_out);
    int64_t max_dst_id = -1, max_timestamp = -1;

    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
      auto src_id_and_atype = it->first;

      edge_out << SuccinctGraph::NODE_ID_DELIM << src_id_and_atype.first;

      edge_out << SuccinctGraph::ATYPE_DELIM << src_id_and_atype.second;

      std::vector<Assoc> assoc_list = it->second;

      max_dst_id = max_timestamp = -1;
      for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
        max_dst_id = std::max(max_dst_id, it2->dst_id);
        max_timestamp = std::max(max_timestamp, it2->time);
      }

      int32_t dst_id_width = num_digits(max_dst_id);
      int32_t edge_width = assoc_list.begin()->attr.length();
      int32_t timestamp_width = num_digits(max_timestamp);

      // output the metadata block:
      // [padded timestamp width; padded dst id width; cnt; edge width]
      edge_out << SuccinctGraph::TIMESTAMP_WIDTH_DELIM
               << pad_timestamp_width(timestamp_width)
               // padded
               << pad_dst_id_width(dst_id_width)  // padded
               << assoc_list.size()  // not padded: so width unbounded
               << SuccinctGraph::EDGE_WIDTH_DELIM << std::to_string(edge_width)  // not padded: so width unbounded
               << SuccinctGraph::METADATA_DELIM;

      COND_LOG_E("timestamp width = %d, max timestamp = %lld\n", timestamp_width,
          max_timestamp);

      // timestamps
      for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
        std::string encoded(encode_timestamp(it2->time, timestamp_width));
        COND_LOG_E("encoded = '%s'\n", encoded.c_str());

        if (decode_timestamp(encoded) != it2->time) {
          LOG_E("Failed: time = [%lld], encoded = [%s], decoded = "
                "[%lld]\n",
                it2->time, encoded.c_str(), decode_timestamp(encoded));
          exit(1);
        }

        edge_out << encoded;
      }
      // dst node ids
      for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
        std::string encoded = encode_node_id(it2->dst_id, dst_id_width);

        if (decode_node_id(encoded) != it2->dst_id) {
          LOG_E("Failed: dst id = [%lld], encoded = [%s], "
                "decoded = [%lld]\n",
                it2->dst_id, encoded.c_str(), decode_timestamp(encoded));
          exit(1);
        }

        edge_out << encoded;
      }
      // edge attributes
      for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
        std::string attr = it2->attr;  // note: no encoding
        if (attr.length() != static_cast<size_t>(edge_width)) {
          LOG_E("Failed: assumption that the edge attr width for each "
                "assoc list is broken: src = %lld, atype = %lld, edge "
                "width = %d, but found attr '%s' (length %d)\n",
                it2->src_id, it2->atype, edge_width, attr.c_str(),
                attr.length());
          exit(1);
        }
        edge_out << attr;
      }
    }

    edge_out << "\n";
  }

  return 0;
}
