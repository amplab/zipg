#include <fstream>
#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"
#include "SuccinctGraphSerde.hpp"
#include "utils.h"

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
      size_t distance = SuccinctGraph::num_digits(node_data.length()) + 1;
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
    GraphFormatter::build_assoc_map(assoc_map, edge_file_in);

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

      int32_t dst_id_width = SuccinctGraph::num_digits(max_dst_id);
      int32_t edge_width = assoc_list.begin()->attr.length();
      int32_t timestamp_width = SuccinctGraph::num_digits(max_timestamp);

      // output the metadata block:
      // [padded timestamp width; padded dst id width; cnt; edge width]
      edge_out << SuccinctGraph::TIMESTAMP_WIDTH_DELIM
               << SuccinctGraphSerde::pad_timestamp_width(timestamp_width)
               // padded
               << SuccinctGraphSerde::pad_dst_id_width(dst_id_width)  // padded
               << assoc_list.size()  // not padded: so width unbounded
               << SuccinctGraph::EDGE_WIDTH_DELIM << std::to_string(edge_width)  // not padded: so width unbounded
               << SuccinctGraph::METADATA_DELIM;

      COND_LOG_E("timestamp width = %d, max timestamp = %lld\n", timestamp_width,
          max_timestamp);

      // timestamps
      for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
        std::string encoded(
            SuccinctGraphSerde::encode_timestamp(it2->time, timestamp_width));
        COND_LOG_E("encoded = '%s'\n", encoded.c_str());

        if (SuccinctGraphSerde::decode_timestamp(encoded) != it2->time) {
          LOG_E("Failed: time = [%lld], encoded = [%s], decoded = "
                "[%lld]\n",
                it2->time, encoded.c_str(),
                SuccinctGraphSerde::decode_timestamp(encoded));
          exit(1);
        }

        edge_out << encoded;
      }
      // dst node ids
      for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
        std::string encoded = SuccinctGraphSerde::encode_node_id(it2->dst_id,
                                                                 dst_id_width);

        if (SuccinctGraphSerde::decode_node_id(encoded) != it2->dst_id) {
          LOG_E("Failed: dst id = [%lld], encoded = [%s], "
                "decoded = [%lld]\n",
                it2->dst_id, encoded.c_str(),
                SuccinctGraphSerde::decode_timestamp(encoded));
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

    // FIXME: without this, SuccinctCore ctor segfaults
    edge_out << "\n";
  }

  return 0;
}
