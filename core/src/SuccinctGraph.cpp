#include "SuccinctGraph.hpp"

#include <limits>
#include <sstream>
#include <thread>

#include "GraphFormatter.hpp"
#include "SuccinctGraphSerde.hpp"
#include "utils.h"

// Controls whether we keep the unstructured input edge table in memory.
#ifdef ENOUGH_MEMORY
#define EDGE_TABLE this->edge_table_with_input_
#else
#define EDGE_TABLE this->edge_table
#endif

#ifdef LOG_DEBUG
#define LOG(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define LOG(fmt, ...)
#endif

// Hacky: represents not-specified query arguments
#define NONE -1

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

// TODO: lots of code duplication among the TAO-like functions

SuccinctGraph::SuccinctGraph(std::string succinct_dir, bool construct,
                             uint32_t sa_sampling_rate,
                             uint32_t isa_sampling_rate,
                             uint32_t npa_sampling_rate) {
  this->edges = -1;
}

SuccinctGraph::SuccinctGraph(std::string node_succinct_dir,
                             std::string edge_succinct_dir) {
  load(node_succinct_dir, edge_succinct_dir);
}

void SuccinctGraph::load(std::string node_succinct_dir,
                         std::string edge_succinct_dir) {
  LOG_E("In SuccinctGraph::load\n");
  this->load_node_table(node_succinct_dir);
  this->load_edge_table(edge_succinct_dir);
  LOG_E("Done SuccinctGraph::load\n");
}

void SuccinctGraph::load_node_table(std::string node_succinct_dir) {
  LOG_E("In SuccinctGraph::load_node_table\n");
  this->node_table = new SuccinctShard(0, node_succinct_dir,
                                       SuccinctMode::LOAD_MEMORY_MAPPED);
  LOG_E("Done SuccinctGraph::load_node_table\n");
}

void SuccinctGraph::load_edge_table(std::string edge_succinct_dir) {
  LOG_E("In SuccinctGraph::load_edge_table\n");
#ifdef ENOUGH_MEMORY
  LOG_E("Keep-input is enabled");
  edge_table = new KeepInputSuccinctFile(
      edge_succinct_dir, SuccinctMode::LOAD_MEMORY_MAPPED);
#else
  edge_table = new SuccinctFile(edge_succinct_dir,
                                SuccinctMode::LOAD_MEMORY_MAPPED);
  load_deleted_edges(edge_succinct_dir + ".deletes");
  // Deserialize deleted edges bitmaps
#endif
  LOG_E("Done SuccinctGraph::load_edge_table\n");
}

void SuccinctGraph::load_deleted_edges(std::string deleted_edges_file) {
  std::ifstream in(deleted_edges_file);
  size_t num_edge_records;
  in.read(reinterpret_cast<char *>(&num_edge_records), sizeof(size_t));
  for (size_t i = 0; i < num_edge_records; i++) {
    int64_t src, atype;
    in.read(reinterpret_cast<char *>(&src), sizeof(int64_t));
    in.read(reinterpret_cast<char *>(&atype), sizeof(int64_t));
    deleted_edges[std::make_pair(src, atype)] = new bitmap::Bitmap();
    deleted_edges[std::make_pair(src, atype)]->Deserialize(in);
  }
}

SuccinctGraph& SuccinctGraph::set_npa_sampling_rate(uint32_t sampling_rate) {
  this->npa_sampling_rate = sampling_rate;
  return *this;
}

SuccinctGraph& SuccinctGraph::set_sa_sampling_rate(uint32_t sampling_rate) {
  this->sa_sampling_rate = sampling_rate;
  return *this;
}

SuccinctGraph& SuccinctGraph::set_isa_sampling_rate(uint32_t sampling_rate) {
  this->isa_sampling_rate = sampling_rate;
  return *this;
}

void SuccinctGraph::construct_node_table(std::string node_file) {
  LOG_E("Constructing node table with npa %d, sa %d, isa %d\n",
        npa_sampling_rate, sa_sampling_rate, isa_sampling_rate);

  // TODO: correct thing to do is use a temp file for this
  // TODO: also, the Succinct dir will have the postfix in it -- not clean?
  std::string formatted_node_file(node_file + "WithPtrs");

  std::ifstream in_stream(node_file);
  std::ofstream out_stream(formatted_node_file);
  std::string line, token;
  std::vector<int64_t> attr_lengths(MAX_NUM_NODE_ATTRS);

  // Output format:
  //
  // distance [delim] len1 [delim] ... lenMAX-1 [delim] [delim1] attr1 ... [delimMAX] attrMAX
  //
  // NOTE:
  //   (1) distance jumps us from right after its [delim] to start of attr1.
  //   (2) to jump to attrK, read from distance up to (& including) len(K-1).

  while (std::getline(in_stream, line)) {
    out_stream << GraphFormatter::attach_attr_lengths(line + "\n");
  }
  out_stream.close();

  this->node_table = new SuccinctShard(0, formatted_node_file,
                                       SuccinctMode::CONSTRUCT_IN_MEMORY,
                                       sa_sampling_rate, isa_sampling_rate,
                                       npa_sampling_rate);
  this->node_table->Serialize();
  LOG_E("Node table constructed and serialized\n");

  // FIXME: rely on some dtor to clean up
  char cmd[99];
  sprintf(cmd, "rm -rf %s", formatted_node_file.c_str());
  LOG_E("Running cmd: '%s'\n", cmd);
  system(cmd);
}

void SuccinctGraph::output_edge_table(const std::string& edge_file,
                                      const std::string& out_file) {
  std::map<std::pair<int64_t, int64_t>, std::vector<Assoc>> assoc_map;
  GraphFormatter::build_assoc_map(assoc_map, edge_file);

  std::ofstream edge_file_out(out_file);
  int64_t max_dst_id = -1, max_timestamp = -1;

  for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
    auto src_id_and_atype = it->first;

    edge_file_out << NODE_ID_DELIM << src_id_and_atype.first;

    edge_file_out << ATYPE_DELIM << src_id_and_atype.second;

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
    edge_file_out << TIMESTAMP_WIDTH_DELIM
                  << SuccinctGraphSerde::pad_timestamp_width(timestamp_width)  // padded
                  << SuccinctGraphSerde::pad_dst_id_width(dst_id_width)  // padded
                  << assoc_list.size()  // not padded: so width unbounded
                  << EDGE_WIDTH_DELIM << std::to_string(edge_width)  // not padded: so width unbounded
                  << METADATA_DELIM;

    COND_LOG_E("timestamp width = %d, max timestamp = %lld\n",
        timestamp_width, max_timestamp);

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

      edge_file_out << encoded;
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

      edge_file_out << encoded;
    }
    // edge attributes
    for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
      std::string attr = it2->attr;  // note: no encoding
      if (attr.length() != static_cast<size_t>(edge_width)) {
        LOG_E("Failed: assumption that the edge attr width for each "
              "assoc list is broken: src = %lld, atype = %lld, edge "
              "width = %d, but found attr '%s' (length %d)\n",
              it2->src_id, it2->atype, edge_width, attr.c_str(), attr.length());
        exit(1);
      }
      edge_file_out << attr;
    }
  }
  // FIXME: without this, SuccinctCore ctor segfaults
  edge_file_out << "\n";
}

void SuccinctGraph::construct_edge_table(std::string edge_file,
                                         bool edge_table_only) {
  // Serialize to an .edge_table file (flat file layout)
  size_t postfix_pos = edge_file.rfind(".assoc");
  std::string edge_file_name = edge_file + ".edge_table";
  if (postfix_pos != std::string::npos) {
    edge_file_name = std::string(edge_file).replace(postfix_pos, 6,
                                                    ".edge_table");
  }

  if (file_or_dir_exists(edge_file_name + ".succinct")) {
    LOG_E("Dir '%s' already exists, exiting normally from construction\n",
          (edge_file_name + ".succinct").c_str());
    return;
  }

  std::map<AssocListKey, std::vector<Assoc>> assoc_map;
  std::string line, token;
  AType atype = -1LL;
  Timestamp time = -1LL;
  NodeId src_id = -1LL, dst_id = -1LL;

  if (!file_or_dir_exists(edge_file_name)) {
    LOG_E("Initializing edge table (SuccinctFile)\n");
    output_edge_table(edge_file, edge_file_name);
    LOG_E("Edge table written out to disk, now to Succinct-encode it\n");
  } else {
    LOG_E("Edge table '%s' exists, skipping\n", edge_file_name.c_str());
  }

  if (edge_table_only) {
    return;
  }

  LOG_E("constructing edge table with npa %d, sa %d, isa %d\n",
        npa_sampling_rate, sa_sampling_rate, isa_sampling_rate);
#ifdef ENOUGH_MEMORY
  EDGE_TABLE = new KeepInputSuccinctFile(edge_file_name,
      SuccinctMode::CONSTRUCT_IN_MEMORY, sa_sampling_rate, npa_sampling_rate);
#else
  EDGE_TABLE = new SuccinctFile(edge_file_name,
                                SuccinctMode::CONSTRUCT_IN_MEMORY,
                                sa_sampling_rate, isa_sampling_rate,
                                npa_sampling_rate);
#endif
  size_t num_bytes = EDGE_TABLE->Serialize();
  LOG_E("Succinct-encoded edge table, number of bytes written: %zu\n",
        num_bytes);
}

void SuccinctGraph::construct(std::string node_file, std::string edge_file) {
  // construct in parallel
  std::thread node_table_thread(&SuccinctGraph::construct_node_table, this,
                                node_file);
  this->construct_edge_table(edge_file);
  node_table_thread.join();

  this->node_file_pathname = node_file;
  this->edge_file_pathname = edge_file;
}

// Note: this is supposed to be used in testing only.
void SuccinctGraph::remove_generated_files() {
  char* cmd = new char[999];
  sprintf(cmd, "rm -rf %s", (this->node_file_pathname + ".succinct").c_str());
  system(cmd);

  sprintf(cmd, "rm -rf %s", (this->edge_file_pathname + ".edge_table").c_str());
  system(cmd);

  sprintf(cmd, "rm -rf %s",
          (this->edge_file_pathname + ".edge_table.succinct").c_str());
  system(cmd);

  delete[] cmd;
}

std::string SuccinctGraph::mk_edge_table_search_key(int64_t src,
                                                    int64_t atype) {
  std::string key(1, NODE_ID_DELIM);
  return key + std::to_string(src) + ATYPE_DELIM + std::to_string(atype)
      + TIMESTAMP_WIDTH_DELIM;
}

std::vector<int64_t> SuccinctGraph::get_edge_table_offsets(NodeId id,
                                                           AType atype) {
  std::vector<int64_t> res;
  std::string key(1, NODE_ID_DELIM);
  COND_LOG_E("In get_edge_table_offsets(%lld, %lld)\n", id, atype);

  if (id == NONE && atype == NONE) {
    EDGE_TABLE->Search(res, key);
  } else if (atype == NONE) {
    EDGE_TABLE->Search(res, key + std::to_string(id) + ATYPE_DELIM);
  } else if (id == NONE) {
    // NOTE: since srcIds are variable-length, this case is same as first
    EDGE_TABLE->Search(res, key);
    // filter by atype
    std::string atype_str;
    uint64_t suf_arr_idx;
    for (auto it = res.begin(); it != res.end();) {
      suf_arr_idx = -1ULL;
      int64_t curr_off = EDGE_TABLE->SkippingExtractUntil(suf_arr_idx, *it,
                                                          ATYPE_DELIM);
      // TODO: extract_compare() similar to SuccinctShard can be nice here
      EDGE_TABLE->ExtractUntil(atype_str, suf_arr_idx, curr_off,
                               TIMESTAMP_WIDTH_DELIM);
      if (std::stol(atype_str) != atype) {
        it = res.erase(it);
      } else {
        ++it;
      }
    }
  } else {
    // case: id & atype are specified.
    key = mk_edge_table_search_key(id, atype);
    COND_LOG_E("About to search for '%s' (size %d) in edge table\n",
        key.c_str(), key.size());
    EDGE_TABLE->Search(res, key);
#ifdef LOG_DEBUG
    COND_LOG_E("search size for (id %lld, atype %lld): %d\n",
        id, atype, res.size());
    for (auto x : res) std::cerr << x << " ";
    std::cerr << std::endl;
#endif
    assert(res.size() <= 1);
  }
  return res;
}

std::vector<SuccinctGraph::Assoc> SuccinctGraph::assoc_range(int64_t src,
                                                             int64_t atype,
                                                             int32_t off,
                                                             int32_t len) {
  COND_LOG_E("assoc_range(src = %lld, atype = %lld, off = %d, len = %d)\n",
      src, atype, off, len);

  if (off == NONE) {
    off = 0;  // extract from start
  }

  std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);
  std::vector<Assoc> result;

  std::string str;

  int32_t len_saved = len, edge_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t suf_arr_idx;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    suf_arr_idx = -1ULL;

    // TODO: should we skip the extract when they are not wildcards?
    // Since the passed-in src and atype can be NONE, extract nonetheless
    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off + 1,  // +1 for skipping NODE_DELIM
                                        ATYPE_DELIM);
    src = std::stoll(str);
    COND_LOG_E("extracted src = %lld, suf_arr_idx = %llu\n",
        src, suf_arr_idx);

    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    atype = std::stoll(str);
    COND_LOG_E("extracted atype = %lld, suf_arr_idx = %llu\n",
        atype, suf_arr_idx);

    EDGE_TABLE->Extract(str, suf_arr_idx, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), suf_arr_idx);
    timestamp_width = std::stoi(str);

    EDGE_TABLE->Extract(
        str, suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), suf_arr_idx);
    dst_id_width = std::stoi(str);

    curr_off = EDGE_TABLE->ExtractUntil(
        str,
        suf_arr_idx,
        // since the last extract() doesn't return the "next" curr_off
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    cnt = std::stoll(str);
    LOG("extracted cnt = %llu\n", cnt);

    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                        METADATA_DELIM);
    edge_width = std::stoi(str);
    LOG("extracted edge width = '%s'\n", str.c_str());

    // if len is wildcard, extract all that's left
    len = std::min(static_cast<int64_t>(len_saved), cnt - off);
    if (len_saved == NONE) {
      len = cnt - off;
    }
    assert(off + len <= cnt);
    if (len <= 0) {
      continue;
    }

    EDGE_TABLE->Extract(str, curr_off + off * timestamp_width,
                        len * timestamp_width);

    std::vector<int64_t> decoded_timestamps =
        SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

    LOG("extracted timestamps = '%s'\n", str.c_str());

    curr_off += cnt * timestamp_width;
    EDGE_TABLE->Extract(str, curr_off + off * dst_id_width, len * dst_id_width);

    std::vector<int64_t> decoded_dst_ids =
        SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

    LOG("extracted dst ids: '%s'\n", str.c_str());

    curr_off += cnt * dst_id_width;
    EDGE_TABLE->Extract(str, curr_off + off * edge_width, len * edge_width);

    LOG("extracted attrs = '%s'\n", str.c_str());

    // Perf comparisons:
    // https://goo.gl/B3Wvj1 - naive
    // https://goo.gl/ckAnB0 - add ctor to Assoc struct, emplace_back w/ it
    // https://goo.gl/zcLovO - don't add ctor, emplace_back() no arg
    for (size_t i = 0; i < decoded_timestamps.size(); ++i) {
      result.emplace_back();
      result.back().src_id = src;
      result.back().dst_id = decoded_dst_ids[i];
      result.back().atype = atype;
      result.back().time = decoded_timestamps[i];
      result.back().attr = std::move(str.substr(i * edge_width, edge_width));
    }LOG("\n");
  }
  return result;
}

int SuccinctGraph::time_range_binary_search_lower_bound(
    Timestamp t_low, int64_t cnt, int64_t curr_off, std::string& tmp_token,
    const int32_t timestamp_width) {
  int l = 0, r = cnt, m;
  Timestamp ts;

  // check t_l >= t_low
  EDGE_TABLE->Extract(tmp_token, curr_off, timestamp_width);
  ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
  if (ts < t_low) {
    return -1;
  }

  // binary search: locates smallest t s.t. t >= t_low
  // invariant: target in [l, r)
  while (l + 1 < r) {
    m = (l + r) / 2;
    EDGE_TABLE->Extract(tmp_token, curr_off + m * timestamp_width,
                        timestamp_width);
    ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
    if (ts >= t_low) {
      l = m;  // note timestamps are decreasing
    } else {
      r = m;
    }
  }
  assert(l + 1 == r);
  return l;
}

int SuccinctGraph::time_range_binary_search_upper_bound(
    Timestamp t_high, int64_t cnt, int64_t curr_off, std::string& tmp_token,
    const int32_t timestamp_width) {
  int l = -1, r = cnt - 1, m;
  Timestamp ts;

  // check t_r <= t_high
  EDGE_TABLE->Extract(tmp_token, curr_off + r * timestamp_width,
                      timestamp_width);
  ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
  if (ts > t_high) {
    return -1;
  }

  while (l + 1 < r) {
    m = (l + r) / 2;
    EDGE_TABLE->Extract(tmp_token, curr_off + m * timestamp_width,
                        timestamp_width);
    ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
    if (ts > t_high) {
      l = m;
    } else {
      r = m;
    }
  }
  assert(l + 1 == r);
  return r;
}

// Basic impl idea: performs binary search on the timestamps.
std::vector<SuccinctGraph::Assoc> SuccinctGraph::assoc_get(
    int64_t src, int64_t atype, const std::set<int64_t>& dst_id_set,
    int64_t t_low, int64_t t_high) {
  COND_LOG_E("assoc_get(src = %" PRId64 ", atype = %" PRId64 ","
      " dstIdSet = ..., tLow = %" PRId64 ", tHigh = %" PRId64 ")\n",
      src, atype, t_low, t_high);

  std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);
  std::vector<Assoc> result;
  std::string str;

  int32_t edge_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t suf_arr_idx;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    suf_arr_idx = -1ULL;

    // Since the passed-in src and atype can be None, extract nonetheless
    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off + 1,
                                        ATYPE_DELIM);  // +1 for skip NODE_DELIM
    LOG("extracted src '%s'\n", str.c_str());
    src = std::stoll(str);

    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    LOG("extracted atype '%s'\n", str.c_str());
    atype = std::stoll(str);

    EDGE_TABLE->Extract(str, suf_arr_idx, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), suf_arr_idx);
    timestamp_width = std::stoi(str);

    EDGE_TABLE->Extract(
        str, suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = EDGE_TABLE->ExtractUntil(
        str,
        suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("extracted cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                        METADATA_DELIM);
    edge_width = std::stoi(str);
    LOG("extracted edge width = '%s'\n", str.c_str());

    int range_left, range_right;  // in-range: [left, right]

    if (t_low != NONE) {
      range_right = time_range_binary_search_lower_bound(t_low, cnt, curr_off,
                                                         str, timestamp_width);
      if (range_right == -1) {
        continue;
      }
    } else {
      // if no time lower bound, just extract all early edges
      range_right = cnt - 1;
    }

    // binary search: locates largest t s.t. t <= t_high
    // invariant: target in (l, r]
    if (t_high != NONE) {
      range_left = time_range_binary_search_upper_bound(t_high, cnt, curr_off,
                                                        str, timestamp_width);
      if (range_left == -1) {
        continue;
      }
    } else {
      // if no time lower bound, just extract all latest edges
      range_left = 0;
    }

    LOG("range left: %d, range right: %d, cnt: %lld\n",
        range_left, range_right, cnt);
    if (range_left > range_right) {
      continue;
    }

    // extract in-range timestamps
    EDGE_TABLE->Extract(str, curr_off + range_left * timestamp_width,
                        (range_right - range_left + 1) * timestamp_width);
    LOG("extracted timestamps = '%s'\n", str.c_str());
    std::vector<int64_t> decoded_timestamps =
        SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

    // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
    curr_off += cnt * timestamp_width;
    EDGE_TABLE->Extract(str, curr_off + range_left * dst_id_width,
                        (range_right - range_left + 1) * dst_id_width);
    LOG("extracted dst ids: '%s'\n", str.c_str());

    std::vector<int64_t> decoded_dst_ids =
        SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

    // filter
    std::vector<int64_t> in_set_indexes;
    for (size_t i = 0; i < decoded_dst_ids.size(); ++i) {
      LOG("decoded_dst_id[i=%d] = %lld\n", i, decoded_dst_ids[i]);

      if (dst_id_set.count(decoded_dst_ids[i]) != 0) {
        in_set_indexes.push_back(range_left + i);
        LOG("pass filter id: %d, decoded_dst_id[i] = %lld\n",
            range_left + i, decoded_dst_ids[i]);
      }
    }

    // TODO: another choice is to do a single extract then filter; evaluate?
    // Now extract only the in-set (and in-range) attrs
    curr_off += cnt * dst_id_width;
    for (int64_t idx : in_set_indexes) {
      result.emplace_back();
      // decoded dst ids and timestamps start w/ absolute idx range_left
      result.back().src_id = src;
      result.back().dst_id = decoded_dst_ids[idx - range_left];
      result.back().atype = atype;
      result.back().time = decoded_timestamps[idx - range_left];
      EDGE_TABLE->Extract(result.back().attr, curr_off + idx * edge_width,
                          edge_width);
    }
  }
  return result;
}

int64_t SuccinctGraph::assoc_count(int64_t src, int64_t atype) {
  COND_LOG_E("In assoc_count(src=%lld, atype=%lld)\n", src, atype);
  std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);
  COND_LOG_E("returned from get eoffs, size: %d\n", eoffs.size());
  int64_t total_cnt = 0;
  std::string str;
  uint64_t suf_arr_idx;

  for (int64_t curr_off : eoffs) {
    suf_arr_idx = -1ULL;
    curr_off = EDGE_TABLE->SkippingExtractUntil(suf_arr_idx, curr_off,
                                                TIMESTAMP_WIDTH_DELIM);

    // "Skip" over the padded timestamp width & padded dst id width anyway
    // This is useful when SuccinctFile is used: saves a sampled ISA lookup
    // by doing 4 extra NPA lookups (assuming the two widths sum to 4)
    EDGE_TABLE->Extract(
        str,
        suf_arr_idx,
        curr_off,
        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);

    // Now extract the count
    EDGE_TABLE->ExtractUntil(
        str,
        suf_arr_idx,
        // + here since last extract() doesn't return an updated offset
        curr_off + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);

    total_cnt += std::stoll(str);
  }
  return total_cnt;
}

std::vector<SuccinctGraph::Assoc> SuccinctGraph::assoc_time_range(
    int64_t src, int64_t atype, int64_t t_low, int64_t t_high, int32_t len) {
  COND_LOG_E("assoc_time_range(src = %lld, atype = %lld, tLow = %lld, "
      "tHigh = %lld, len = %d)\n",
      src, atype, t_low, t_high, len);

  std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);
  std::vector<Assoc> result;
  std::string str;

  int32_t edge_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t suf_arr_idx;

  int32_t len_saved = len;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    suf_arr_idx = -1ULL;

    // Since the passed-in src and atype can be None, extract nonetheless
    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off + 1,
                                        ATYPE_DELIM);  // +1 for skip NODE_DELIM
    src = std::stoll(str);

    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    atype = std::stoll(str);

    EDGE_TABLE->Extract(str, suf_arr_idx, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), suf_arr_idx);
    timestamp_width = std::stoi(str);

    EDGE_TABLE->Extract(
        str, suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = EDGE_TABLE->ExtractUntil(
        str,
        suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("extracted cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                        METADATA_DELIM);
    edge_width = std::stoi(str);
    LOG("extracted edge width = '%s'\n", str.c_str());

    int range_left, range_right;  // in-range: [left, right]

    if (t_low != NONE) {
      range_right = time_range_binary_search_lower_bound(t_low, cnt, curr_off,
                                                         str, timestamp_width);
      if (range_right == -1) {
        continue;
      }
    } else {
      // if no time lower bound, just extract all early edges
      range_right = cnt - 1;
    }

    // binary search: locates largest t s.t. t <= t_high
    // invariant: target in (l, r]
    if (t_high != NONE) {
      range_left = time_range_binary_search_upper_bound(t_high, cnt, curr_off,
                                                        str, timestamp_width);
      if (range_left == -1) {
        continue;
      }
    } else {
      // if no time lower bound, just extract all latest edges
      range_left = 0;
    }

    // if len is wildcard, extract all that's left
    if (len_saved == NONE) {
      len = cnt;
    }
    // limit to first `len` edges
    range_right = std::min(range_right, range_left + len - 1);

    LOG("range left: %d, range right: %d, cnt: %lld\n",
        range_left, range_right, cnt);
    if (range_left > range_right) {
      continue;
    }

    // extract in-range timestamps
    EDGE_TABLE->Extract(str, curr_off + range_left * timestamp_width,
                        (range_right - range_left + 1) * timestamp_width);
    LOG("extracted timestamps = '%s'\n", str.c_str());
    std::vector<int64_t> decoded_timestamps =
        SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

    // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
    curr_off += cnt * timestamp_width;
    EDGE_TABLE->Extract(str, curr_off + range_left * dst_id_width,
                        (range_right - range_left + 1) * dst_id_width);
    LOG("extracted dst ids: '%s'\n", str.c_str());
    std::vector<int64_t> decoded_dst_ids =
        SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

    // TODO: another choice is to do a single extract then filter; evaluate?
    // Now extract only the in-set (and in-range) attrs
    curr_off += cnt * dst_id_width;
    for (size_t i = 0; i < decoded_timestamps.size(); ++i) {
      result.emplace_back();
      // decoded dst ids and timestamps start w/ absolute idx range_left
      result.back().src_id = src;
      result.back().dst_id = decoded_dst_ids[i];
      result.back().atype = atype;
      result.back().time = decoded_timestamps[i];
      EDGE_TABLE->Extract(result.back().attr,
                          curr_off + (range_left + i) * edge_width, edge_width);
    }
  }
  return result;
}

std::string SuccinctGraph::succinct_directory() {
  return this->succinct_dir;
}

int64_t SuccinctGraph::num_nodes() {
  return this->node_table->GetNumKeys() - 1;  // FIXME: a bug in SuccinctShard
}

int64_t SuccinctGraph::num_edges() {
  return edges;
}

size_t SuccinctGraph::storage_size() {
  assert(false && "not implemented");
  return -1;
}

size_t SuccinctGraph::serialize() {
  assert(false && "not implemented");
  return -1;
}

/******* Primitive APIs *******/

// This might not be most efficient as we don't jump over the lengths.
void SuccinctGraph::get_attribute(std::string& result, int64_t node_id,
                                  int attr) {
  assert(attr < MAX_NUM_NODE_ATTRS);
  uint64_t suf_arr_idx = -1ULL;

  const char next_attr_delim = static_cast<char>(DELIMITERS[attr + 1]);
  std::string tmp;
  int64_t start_offset = this->node_table->ExtractUntil(
      tmp, suf_arr_idx, node_id, NODE_TABLE_HEADER_DELIM);

  COND_LOG_E("extracted node table header '%s'\n", tmp.c_str());

  if (start_offset == -1) {
    result.clear();
    return;  // key doesn't exist
  }
  // +(attr + 1) to account for delims after each of the lengths
  int32_t dist = std::stoi(tmp) + (attr + 1);

  for (int i = 1; i <= attr; ++i) {
    this->node_table->ExtractUntil(tmp, suf_arr_idx, node_id,
                                   NODE_TABLE_HEADER_DELIM);
    COND_LOG_E("extracted length '%s'\n", tmp.c_str());
    dist += std::stoi(tmp);
  }

  // jump!
  this->node_table->ExtractUntil(result, start_offset + dist, next_attr_delim);
}

inline void SuccinctGraph::extract_neighbors(
    std::vector<int64_t>& result, const std::vector<int64_t>& offsets,
    int32_t skip_length) {
  result.clear();
  std::string str;
  uint64_t suf_arr_idx;
  int32_t dst_id_width, timestamp_width;
  int64_t cnt;

  for (int64_t curr_off : offsets) {
    suf_arr_idx = -1ULL;

    curr_off = EDGE_TABLE->SkippingExtractUntil(suf_arr_idx,
                                                curr_off + skip_length,
                                                TIMESTAMP_WIDTH_DELIM);

    EDGE_TABLE->Extract(str, suf_arr_idx, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    timestamp_width = std::stoi(str);

    EDGE_TABLE->Extract(
        str, suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = EDGE_TABLE->ExtractUntil(
        str,
        suf_arr_idx,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = EDGE_TABLE->SkippingExtractUntil(suf_arr_idx, curr_off,
                                                METADATA_DELIM);

    EDGE_TABLE->Extract(str, curr_off + cnt * timestamp_width,
                        cnt * dst_id_width);
    LOG("dst ids = '%s'\n", str.c_str());

#ifdef BYTES_EXTRACTED
    bytes_extracted += cnt * dst_id_width;
#endif

    std::vector<int64_t> decoded(
        SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width));

    result.insert(result.end(), decoded.begin(), decoded.end());
  }

#ifdef BYTES_EXTRACTED
  printf(",%lld\n", bytes_extracted);
#endif
}

void SuccinctGraph::extract_edge_attrs(std::vector<std::string>& result,
                                       int64_t curr_off, int32_t skip_length) {
  std::string str;
  uint64_t suf_arr_idx = -1ULL;

  curr_off = EDGE_TABLE->SkippingExtractUntil(suf_arr_idx,
                                              curr_off + skip_length,
                                              TIMESTAMP_WIDTH_DELIM);

  EDGE_TABLE->Extract(str, suf_arr_idx, curr_off,
                      SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
  const int32_t timestamp_width = std::stoi(str);

  EDGE_TABLE->Extract(
      str, suf_arr_idx,
      curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
      SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
  LOG("dst id width = '%s'\n", str.c_str());
  const int32_t dst_id_width = std::stoi(str);

  curr_off = EDGE_TABLE->ExtractUntil(
      str,
      suf_arr_idx,
      curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
          + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
      EDGE_WIDTH_DELIM);
  LOG("cnt = '%s'\n", str.c_str());
  const int64_t cnt = std::stoll(str);

  curr_off = EDGE_TABLE->ExtractUntil(str, suf_arr_idx, curr_off,
                                      METADATA_DELIM);
  const int32_t edge_attr_width = std::stoi(str);

  EDGE_TABLE->Extract(str, curr_off + cnt * (timestamp_width + dst_id_width),
                      cnt * edge_attr_width);
  LOG("attrs = '%s'\n", str.c_str());

  result.resize(cnt);
  for (size_t i = 0; i < cnt; ++i) {
    result[i] = std::move(str.substr(i * edge_attr_width, edge_attr_width));
  }
}

void SuccinctGraph::get_edge_attrs(std::vector<std::string>& result,
                                   int64_t node, int64_t atype) {
  result.clear();
  std::vector<int64_t> offsets;
  EDGE_TABLE->Search(
      offsets,
      NODE_ID_DELIM + std::to_string(node) + ATYPE_DELIM + std::to_string(atype)
          + TIMESTAMP_WIDTH_DELIM);
  assert(offsets.size() <= 1);
  if (offsets.size() == 1) {
    // skip node delim, node, atype delim
    extract_edge_attrs(result, offsets[0], num_digits(node) + 2);
  }
}

void SuccinctGraph::get_neighbors(std::vector<int64_t>& result, int64_t node) {

#ifdef DEBUG_TIME_NHBR1
  auto t1 = get_timestamp();
#endif

  std::vector<int64_t> offsets;
  EDGE_TABLE->Search(offsets,
                     NODE_ID_DELIM + std::to_string(node) + ATYPE_DELIM);

#ifdef DEBUG_TIME_NHBR1
  auto t2 = get_timestamp();
  printf(".%lld\n", t2 - t1);
  t1 = get_timestamp();
#endif

  // skip node delim, node, atype delim
  extract_neighbors(result, offsets, num_digits(node) + 2);

#ifdef DEBUG_TIME_NHBR1
  t2 = get_timestamp();
  printf(",%lld\n", t2 - t1);
#endif
}

void SuccinctGraph::filter_nodes(std::vector<int64_t>& result,
                                 const std::vector<int64_t>& node_ids, int attr,
                                 const std::string& search_key) {
  COND_LOG_E("in graph filter_nodes(.., attr %d, key '%s')\n",
      attr, search_key.c_str());

  assert(attr < SuccinctGraph::MAX_NUM_NODE_ATTRS);
  result.clear();
  const char next_attr_delim = static_cast<char>(DELIMITERS[attr + 1]);
  std::string tmp;

  for (int64_t node_id : node_ids) {
    COND_LOG_E("node %lld\n", node_id);

    uint64_t suf_arr_idx = -1ULL;
    int64_t start_offset = this->node_table->ExtractUntil(
        tmp, suf_arr_idx, node_id, NODE_TABLE_HEADER_DELIM);

    COND_LOG_E("extracted node table header '%s'\n", tmp.c_str());

    if (start_offset == -1)
      continue;  // key doesn't exist
    // +(attr + 1) to account for delims after each of the lengths
    int32_t dist = std::stoi(tmp) + (attr + 1);

    for (int i = 1; i <= attr; ++i) {
      this->node_table->ExtractUntil(tmp, suf_arr_idx, node_id,
                                     NODE_TABLE_HEADER_DELIM);
      COND_LOG_E("extracted length '%s'\n", tmp.c_str());
      dist += std::stoi(tmp);
    }

    // jump!
    if (this->node_table->ExtractCompareUntil(start_offset + dist,
                                              next_attr_delim, search_key)) {
      COND_LOG_E("compared successfully, keeping id %lld\n", node_id);
      result.push_back(node_id);
    }
  }
}

void SuccinctGraph::obj_get(std::vector<std::string>& results, int64_t obj_id) {
  std::string token;
  uint64_t suf_arr_idx = -1ULL;
  int64_t start_offset = this->node_table->ExtractUntil(
      token, suf_arr_idx, obj_id, NODE_TABLE_HEADER_DELIM);

  // FIXME: due to a bug in SuccinctShard (or perhaps the way we use it), node
  // `num_nodes()` will incorrectly have an entry (empty string). Therefore we
  // special-case for now.
  if (start_offset == -1 || obj_id == num_nodes()) {
    results.clear();
    return;  // key doesn't exist
  }

  results.resize(MAX_NUM_NODE_ATTRS);
  int32_t dist = std::stoi(token) + 1;  // +1 for delim
  int64_t curr_off = start_offset + dist;
  int last_non_empty = -1;
  suf_arr_idx = -1ULL;
  for (int attr = 0; attr < MAX_NUM_NODE_ATTRS; ++attr) {
    this->node_table->ExtractUntilHint(results[attr], suf_arr_idx, curr_off,
                                       static_cast<char>(DELIMITERS[attr + 1]));
    if (!results[attr].empty()) {
      last_non_empty = attr;
    }
  }
  results.resize(last_non_empty + 1);
}

// Scans neighbors and looks for those that contain the desired attr.
void SuccinctGraph::get_neighbors(std::vector<int64_t>& result, int64_t node_id,
                                  int attr, const std::string& search_key) {

#ifdef DEBUG_TIME_NHBR2
  auto t1 = get_timestamp();
#endif

  assert(attr < SuccinctGraph::MAX_NUM_NODE_ATTRS);
  std::vector<int64_t> nbhrs;
  get_neighbors(nbhrs, node_id);

#ifdef DEBUG_TIME_NHBR2
  auto t2 = get_timestamp();
  printf(".%lld\n", t2 - t1);
  if (t2 - t1 == 0) assert(nbhrs.empty());
  t1 = get_timestamp();
#endif

  filter_nodes(result, nbhrs, attr, search_key);

#ifdef DEBUG_TIME_NHBR2
  t2 = get_timestamp();
  printf(",%lld\n", t2 - t1);
#endif
}

void SuccinctGraph::get_neighbors(std::vector<int64_t>& result, int64_t node,
                                  int64_t atype) {

#ifdef DEBUG_TIME_NHBR3
  auto t1 = get_timestamp();
#endif

  std::vector<int64_t> offsets;
  EDGE_TABLE->Search(
      offsets,
      NODE_ID_DELIM + std::to_string(node) + ATYPE_DELIM + std::to_string(atype)
          + TIMESTAMP_WIDTH_DELIM);

#ifdef DEBUG_TIME_NHBR3
  auto t2 = get_timestamp();
  printf(".%lld\n", t2 - t1);
  t1 = get_timestamp();
#endif

  // skip 2 delims & node & atype, i.e. first ISA lookup will hit dst id delim
  extract_neighbors(result, offsets, num_digits(node) + num_digits(atype) + 2);

#ifdef DEBUG_TIME_NHBR3
  t2 = get_timestamp();
  printf(",%lld\n", t2 - t1);
#endif
}

void SuccinctGraph::get_nodes(std::set<int64_t>& result, int attr,
                              const std::string& search_key) {

  result.clear();
  this->node_table->Search(result, mk_node_attr_key(attr, search_key));
}

void SuccinctGraph::get_nodes(std::set<int64_t>& result, int attr1,
                              const std::string& search_key1, int attr2,
                              const std::string& search_key2) {

  result.clear();
  std::set<int64_t> s1, s2;
  this->node_table->Search(s1, mk_node_attr_key(attr1, search_key1));
  this->node_table->Search(s2, mk_node_attr_key(attr2, search_key2));
  // result.end() is a hint that supposedly is faster than .begin()
  std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                        std::inserter(result, result.end()));
}

// LinkBench API
bool SuccinctGraph::getNode(std::string& data, int64_t id) {
  std::string token;
  uint64_t suf_arr_idx = -1ULL;
  int64_t start_offset = this->node_table->ExtractUntil(
      token, suf_arr_idx, id, NODE_TABLE_HEADER_DELIM);

  // FIXME: due to a bug in SuccinctShard (or perhaps the way we use it), node
  // `num_nodes()` will incorrectly have an entry (empty string). Therefore we
  // special-case for now.
  if (start_offset == -1 || id == num_nodes()) {
    return false;  // key doesn't exist
  }

  int32_t dist = std::stoi(token) + 1;  // +1 for delim
  int64_t curr_off = start_offset + dist;
  int last_non_empty = -1;
  suf_arr_idx = -1ULL;

  node_table->ExtractUntilHint(data, suf_arr_idx, curr_off, '\n');

  return true;
}

bool SuccinctGraph::getLink(Link& link, int64_t id1, int64_t link_type,
                            int64_t id2) {
  COND_LOG_E("getLink(id1=%lld, link_type=%lld, id2=%lld)\n",
      id1, link_type, id2);

  std::vector<int64_t> eoffs = get_edge_table_offsets(id1, link_type);
  std::vector<Assoc> result;
  std::string str;

  int32_t edge_data_len_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t idx_hint;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    idx_hint = -1ULL;

    // Since the passed-in src and atype can be None, extract nonetheless
    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off + 1,
                                        ATYPE_DELIM);  // +1 for skip NODE_DELIM
    int64_t id1_extracted = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    int64_t link_type_extracted = std::stoll(str);

    edge_table->Extract(str, idx_hint, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), sa_idx);
    timestamp_width = std::stoi(str);

    edge_table->Extract(
        str, idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = edge_table->ExtractUntil(
        str,
        idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("extracted cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        METADATA_DELIM);
    edge_data_len_width = std::stoi(str);
    LOG("extracted edge data length width = '%s'\n", str.c_str());

    // Save timestamp offset
    uint64_t timestamp_off = curr_off;

    // Skip to dst-ids
    curr_off += (cnt * timestamp_width);

    int64_t idx;
    for (idx = 0; idx < cnt; idx++) {
      edge_table->Extract(str, curr_off + idx * dst_id_width, dst_id_width);
      if (std::stoll(str) == id2)
        break;
    }

    if (idx == cnt
        || deleted_edges[std::make_pair(id1, link_type)]->GetBit(idx))
      continue;

    // Populate link data
    link.src_id = id1;
    link.atype = link_type;
    link.dst_id = id2;
    edge_table->Extract(str, timestamp_off + idx * timestamp_width,
                        timestamp_width);
    link.time = std::stoll(str);
    curr_off += cnt * dst_id_width;
    for (int64_t prop_idx = 0; prop_idx < idx; prop_idx++) {
      edge_table->Extract(str, curr_off, edge_data_len_width);
      curr_off += (edge_data_len_width + std::stoll(str));
    }
    edge_table->Extract(str, curr_off, edge_data_len_width);
    int64_t prop_len = std::stoll(str);
    edge_table->Extract(link.attr, curr_off + edge_data_len_width, prop_len);

    return true;
  }

  return false;
}

void SuccinctGraph::getLinkList(std::vector<Link>& assocs, int64_t id1,
                                int64_t link_type) {
  COND_LOG_E("getLinkList(id1=%lld, link_type=%lld)\n", id1, link_type);

  std::vector<int64_t> eoffs = get_edge_table_offsets(id1, link_type);
  std::string str;

  int32_t edge_data_len_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t idx_hint;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    idx_hint = -1ULL;

    // Since the passed-in src and atype can be None, extract nonetheless
    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off + 1,
                                        ATYPE_DELIM);  // +1 for skip NODE_DELIM
    int64_t id1_extracted = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    int64_t link_type_extracted = std::stoll(str);

    edge_table->Extract(str, idx_hint, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), sa_idx);
    timestamp_width = std::stoi(str);

    edge_table->Extract(
        str, idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = edge_table->ExtractUntil(
        str,
        idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("extracted cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        METADATA_DELIM);
    edge_data_len_width = std::stoi(str);
    LOG("extracted edge data length width = '%s'\n", str.c_str());

    edge_table->Extract(str, curr_off, cnt * timestamp_width);

    std::vector<int64_t> decoded_timestamps =
        SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

    LOG("extracted timestamps = '%s'\n", str.c_str());

    curr_off += cnt * timestamp_width;
    EDGE_TABLE->Extract(str, curr_off, cnt * dst_id_width);

    std::vector<int64_t> decoded_dst_ids =
        SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

    LOG("extracted dst ids: '%s'\n", str.c_str());

    curr_off += cnt * dst_id_width;
    for (size_t i = 0; i < cnt; ++i) {
      assocs.emplace_back();
      assocs.back().src_id = id1;
      assocs.back().dst_id = decoded_dst_ids[i];
      assocs.back().atype = link_type;
      assocs.back().time = decoded_timestamps[i];
      edge_table->Extract(str, curr_off, edge_data_len_width);
      int64_t prop_len = std::stoll(str);
      edge_table->Extract(assocs.back().attr, curr_off + edge_data_len_width,
                          prop_len);
      curr_off += (edge_data_len_width + prop_len);

      if (deleted_edges[std::make_pair(id1, link_type)]->GetBit(i)) {
        assocs.pop_back();
      }
    }
  }
}

void SuccinctGraph::getLinkList(std::vector<Link>& assocs, int64_t id1,
                                uint64_t link_type, int64_t min_timestamp,
                                int64_t max_timestamp, int64_t offset,
                                int64_t limit) {
  COND_LOG_E("getLinkList(id1=%lld, link_type=%lld, min_timestamp=%lld, max_timestamp=%lld, offset=%lld, limit=%lld)\n",
      id1, link_type, min_timestamp, max_timestamp, offset, limit);

  std::vector<int64_t> eoffs = get_edge_table_offsets(id1, link_type);
  std::string str;

  int32_t edge_data_len_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t idx_hint;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    idx_hint = -1ULL;

    // Since the passed-in src and atype can be None, extract nonetheless
    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off + 1,
                                        ATYPE_DELIM);  // +1 for skip NODE_DELIM
    int64_t id1_extracted = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    int64_t link_type_extracted = std::stoll(str);

    edge_table->Extract(str, idx_hint, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), sa_idx);
    timestamp_width = std::stoi(str);

    edge_table->Extract(
        str, idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = edge_table->ExtractUntil(
        str,
        idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("extracted cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        METADATA_DELIM);
    edge_data_len_width = std::stoi(str);
    LOG("extracted edge data length width = '%s'\n", str.c_str());

    int64_t range_left, range_right;  // in-range: [left, right]

    range_right = time_range_binary_search_lower_bound(min_timestamp, cnt,
                                                       curr_off, str,
                                                       timestamp_width);
    if (range_right == -1) {
      continue;
    }

    // binary search: locates largest timestamp t s.t. t <= max_timestamp
    // invariant: target in (l, r]
    range_left = time_range_binary_search_upper_bound(max_timestamp, cnt,
                                                      curr_off, str,
                                                      timestamp_width);
    if (range_left == -1) {
      continue;
    }

    LOG("range left: %d, range right: %d, cnt: %lld\n",
        range_left, range_right, cnt);

    int64_t lo = range_left + offset;
    int64_t hi = range_right;
    if (range_left > range_right) {
      continue;
    }

    // extract in-range timestamps
    edge_table->Extract(str, curr_off + lo * timestamp_width,
                        (hi - lo + 1) * timestamp_width);
    LOG("extracted timestamps = '%s'\n", str.c_str());
    std::vector<int64_t> decoded_timestamps =
        SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

    // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
    curr_off += cnt * timestamp_width;
    edge_table->Extract(str, curr_off + lo * dst_id_width,
                        (hi - lo + 1) * dst_id_width);
    LOG("extracted dst ids: '%s'\n", str.c_str());

    std::vector<int64_t> decoded_dst_ids =
        SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

    curr_off += cnt * dst_id_width;
    for (size_t i = 0; i <= hi && assocs.size() < limit; ++i) {
      if (i < lo) {
        edge_table->Extract(str, curr_off, edge_data_len_width);
        curr_off += (edge_data_len_width + std::stoll(str));
      } else {
        assocs.emplace_back();
        assocs.back().src_id = id1;
        assocs.back().dst_id = decoded_dst_ids[i];
        assocs.back().atype = link_type;
        assocs.back().time = decoded_timestamps[i];
        edge_table->Extract(str, curr_off, edge_data_len_width);
        int64_t prop_len = std::stoll(str);
        edge_table->Extract(assocs.back().attr, curr_off + edge_data_len_width,
                            prop_len);
        curr_off += (edge_data_len_width + prop_len);
        if (deleted_edges[std::make_pair(id1, link_type)]->GetBit(i))
          assocs.pop_back();
      }
    }
  }
}

int64_t SuccinctGraph::countLinks(int64_t id1, int64_t link_type) {
  return assoc_count(id1, link_type);
}

bool SuccinctGraph::deleteNode(int64_t id) {
  return node_table->Delete(id);
}

bool SuccinctGraph::deleteLink(int64_t id1, int64_t link_type, int64_t id2) {
  COND_LOG_E("deleteLink(id1=%lld, link_type=%lld, id2=%lld)\n",
      id1, link_type, id2);

  std::vector<int64_t> eoffs = get_edge_table_offsets(id1, link_type);
  std::vector<Assoc> result;
  std::string str;

  int32_t edge_data_len_width, dst_id_width, timestamp_width;
  int64_t cnt;
  uint64_t idx_hint;

  for (int64_t curr_off : eoffs) {
    LOG("edge table offset = %llu\n", curr_off);
    idx_hint = -1ULL;

    // Since the passed-in src and atype can be None, extract nonetheless
    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off + 1,
                                        ATYPE_DELIM);  // +1 for skip NODE_DELIM
    int64_t id1_extracted = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        TIMESTAMP_WIDTH_DELIM);
    int64_t link_type_extracted = std::stoll(str);

    edge_table->Extract(str, idx_hint, curr_off,
                        SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
    LOG("extracted timestamp width = '%s', suf_arr_idx = %llu\n",
        str.c_str(), sa_idx);
    timestamp_width = std::stoi(str);

    edge_table->Extract(
        str, idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
        SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
    LOG("extracted dst id width = '%s'\n", str.c_str());
    dst_id_width = std::stoi(str);

    curr_off = edge_table->ExtractUntil(
        str,
        idx_hint,
        curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED
            + SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
        EDGE_WIDTH_DELIM);
    LOG("extracted cnt = '%s'\n", str.c_str());
    cnt = std::stoll(str);

    curr_off = edge_table->ExtractUntil(str, idx_hint, curr_off,
                                        METADATA_DELIM);
    edge_data_len_width = std::stoi(str);
    LOG("extracted edge data length width = '%s'\n", str.c_str());

    // Save timestamp offset
    uint64_t timestamp_off = curr_off;

    // Skip to dst-ids
    curr_off += (cnt * timestamp_width);

    int64_t idx;
    for (idx = 0; idx < cnt; idx++) {
      edge_table->Extract(str, curr_off + idx * dst_id_width, dst_id_width);
      if (std::stoll(str) == id2)
        break;
    }

    if (idx == cnt)
      continue;

    if (!deleted_edges[edge_record_id_t(id1, link_type)]->GetBit(idx)) {
      deleted_edges[edge_record_id_t(id1, link_type)]->SetBit(idx);
      return true;
    }
  }

  return false;
}
