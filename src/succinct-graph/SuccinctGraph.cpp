#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/SuccinctGraphSerde.hpp"

#include <limits>
#include <sstream>

// TODO: remove
const int NUM_ATTRIBUTES = 10;

// FIXME
const char NODE_ID_DELIM = '\x02';
const char ATYPE_DELIM = '\x03';
//const char NODE_ID_DELIM = 'A';
//const char ATYPE_DELIM = 'B';
const std::string SuccinctGraph::DELIMINATORS = "<>()#$%&*+[]{}^-|~;? \"',./:=@\\_~\x02\x03\x04\x05\x06\x07\x08\x09";

SuccinctGraph::SuccinctGraph(
    std::string succinct_dir,
    bool construct,
    uint32_t sa_sampling_rate,
    uint32_t isa_sampling_rate,
    uint32_t npa_sampling_rate) {
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

SuccinctGraph& SuccinctGraph::construct(
    std::string node_file,
    std::string edge_file) {

    fprintf(stderr, "Initializing node table (SuccinctShard)\n");

    // TODO: needs to call delete on the allocated object?
    this->node_table = new SuccinctShard(
        0,
        node_file,
        SuccinctMode::CONSTRUCT_IN_MEMORY,
        sa_sampling_rate,
        isa_sampling_rate,
        npa_sampling_rate
    );
    this->node_table->serialize();

    fprintf(stderr, "Initializing edge table (SuccinctFile)\n");

    std::map<AssocListKey, std::vector<Assoc>> assoc_map;
    std::string line, token;
    std::ifstream edge_file_stream(edge_file);
    AType atype;
    Timestamp time;
    NodeId src_id, dst_id;

    while (std::getline(edge_file_stream, line)) {
        std::stringstream ss(line);
        int token_idx = 0;
        while (std::getline(ss, token, ' ')) {
            ++token_idx;
            if (token_idx == 1) src_id = std::stoi(token);
            else if (token_idx == 2) dst_id = std::stoi(token);
            else if (token_idx == 3) atype = std::stoi(token);
            else if (token_idx == 4) time = std::stoi(token);
            token.clear();
            if (token_idx == 4) break;
        }
        std::getline(ss, token); // rest of the data is attr
        Assoc assoc = { dst_id, time, token };
        assoc_map[std::make_pair(src_id, atype)].push_back(assoc);
    }
    edge_file_stream.close();

    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        std::sort(it->second.begin(),
                  it->second.end(),
                  cmp_assoc_by_decreasing_time);
    }

    // just debug messages
//    printf("\n");
    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        std::vector<Assoc> assocs = it->second;
        printf("[node %lld, atype %d]: ",
            (it->first).first, (it->first).second);
        for (auto it2 = assocs.begin(); it2 != assocs.end(); ++it2) {
            printf(" (dst %lld, time %lld, attr '%s')",
                   it2->dst_id,
                   it2->time,
                   (it2->attr).c_str());
        }
        printf("\n");
    }
    printf("\n");

    // Serialize to an .edge_table file (flat file layout)

    std::string edge_file_name = edge_file.replace(
        edge_file.rfind(".assoc"), 6, ".edge_table");
    std::ofstream edge_file_out(edge_file_name);

    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        auto src_id_and_atype = it->first;

        edge_file_out << NODE_ID_DELIM
            << SuccinctGraphSerde::pad_int64(src_id_and_atype.first);

        edge_file_out << ATYPE_DELIM
            << SuccinctGraphSerde::pad_int32(src_id_and_atype.second);

        std::vector<Assoc> assoc_list = it->second;

        int32_t edge_width = assoc_list.begin()->attr.length();
        int64_t data_width = assoc_list.size() *
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);

        edge_file_out << SuccinctGraphSerde::pad_int32(edge_width)
            << SuccinctGraphSerde::pad_int64(data_width);

        // timestamps
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            std::string encoded = SuccinctGraphSerde::encode_int64(
                it2->time, WIDTH_TIMESTAMP);

            if (SuccinctGraphSerde::decode_int64(encoded) != it2->time) {
                printf("Failed: time = [%lld], encoded = [%s], decoded = [%lld]\n",
                    it2->time, encoded.c_str(), SuccinctGraphSerde::decode_int64(encoded));
                assert(0);
            }

            edge_file_out << encoded;
        }
        // dst node ids
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            std::string encoded = SuccinctGraphSerde::encode_int64(
                it2->dst_id, WIDTH_TIMESTAMP);

            if (SuccinctGraphSerde::decode_int64(encoded) != it2->dst_id) {
                printf("Failed: dst_id = [%lld], encoded = [%s], decoded = [%lld]\n",
                    it2->dst_id, encoded.c_str(), SuccinctGraphSerde::decode_int64(encoded));
                assert(0);
            }

            edge_file_out << encoded;
        }
        // edge attributes
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            std::string attr = it2->attr; // note: no encoding
            assert(attr.length() == edge_width);
            edge_file_out << attr;
        }
    }
    edge_file_out << "\n"; // FIXME: without this, SuccinctCore ctor segfaults
    edge_file_out.close();
    printf("Edge table written out to disk, now to Succinct-encode it\n");

    this->edge_table = new SuccinctFile(edge_file_name);
//    printf("Constructed\n");
    size_t num_bytes = this->edge_table->serialize();
    printf("Succinct-encoded edge table, number of bytes written: %zu\n",
        num_bytes);

    return *this;
}

SuccinctGraph& SuccinctGraph::load(
    std::string node_succinct_dir,
    std::string edge_succinct_dir) {

    this->node_table = new SuccinctShard(
        0,
        node_succinct_dir,
        SuccinctMode::LOAD_MEMORY_MAPPED);

    this->edge_table = new SuccinctFile(
        edge_succinct_dir,
        SuccinctMode::LOAD_MEMORY_MAPPED);

    return *this;
}

uint64_t SuccinctGraph::get_edge_table_offset(NodeId id, AType atype) {
    std::vector<int64_t> res;
    std::string key(1, NODE_ID_DELIM);
    key += SuccinctGraphSerde::pad_int64(id) + ATYPE_DELIM +
        SuccinctGraphSerde::pad_int32(atype);
    // printf("search key = '%s'\n", key.c_str());
    this->edge_table->search(res, key);
    assert(res.size() == 1);
    return res.at(0);
}

void SuccinctGraph::obj_get(std::string& result, int64_t obj_id) {
    this->node_table->get(result, obj_id);
}

std::vector<SuccinctGraph::AssocResult> SuccinctGraph::assoc_range(
    int64_t src, int32_t atype, int32_t off, int32_t len) {

    printf("assoc_range(src = %lld, atype = %d, off = %d, len = %d): ",
        src, atype, off, len);
    uint64_t curr_off = get_edge_table_offset(src, atype);
    printf("edge table offset = %llu\n", curr_off);

    curr_off += 1 + // node delim
        SuccinctGraphSerde::WIDTH_INT64_PADDED + // padded node id
        1 + // atype delim
        SuccinctGraphSerde::WIDTH_INT32_PADDED; // padded atype

    std::string edge_width_str;
    this->edge_table->extract(
        edge_width_str, curr_off, SuccinctGraphSerde::WIDTH_INT32_PADDED);
    printf("extracted edge width = '%s'\n", edge_width_str.c_str());
    int edge_width = std::stoi(edge_width_str); // TODO: int is the right type?

    std::string data_width;
    curr_off += SuccinctGraphSerde::WIDTH_INT32_PADDED;
    this->edge_table->extract(
        data_width, curr_off, SuccinctGraphSerde::WIDTH_INT64_PADDED);
    printf("extracted data width = '%s'\n", data_width.c_str());

    std::string timestamps;
    curr_off += SuccinctGraphSerde::WIDTH_INT64_PADDED;
    this->edge_table->extract(timestamps,
                              curr_off + off * WIDTH_TIMESTAMP,
                              len * WIDTH_TIMESTAMP);
    printf("extracted timestamps = '%s'\n", timestamps.c_str());

    assert(std::stoi(data_width) %
        (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width) == 0);
    uint64_t cnt = std::stoi(data_width) * 1.0 /
        (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);
    assert(off + len <= cnt);
    printf("cnt = %llu\n", cnt);

    curr_off += cnt * WIDTH_TIMESTAMP;
    std::string dst_ids;
    this->edge_table->extract(dst_ids,
                              curr_off + off * WIDTH_NODE_ID,
                              len * WIDTH_NODE_ID);
    printf("extracted dst ids: '%s'\n", dst_ids.c_str());

    curr_off += cnt * WIDTH_NODE_ID;
    std::string attrs;
    this->edge_table->extract(attrs,
                              curr_off + off * edge_width,
                              len * edge_width);
    printf("extracted attrs = '%s'\n", attrs.c_str());

    std::vector<int64_t> decoded_timestamps =
        SuccinctGraphSerde::decode_multi_int64(timestamps, WIDTH_TIMESTAMP);
    std::vector<int64_t> decoded_dst_ids =
        SuccinctGraphSerde::decode_multi_int64(dst_ids, WIDTH_NODE_ID);
    std::vector<AssocResult> result;
    for (int i = 0; i < decoded_timestamps.size(); ++i) {
        result.push_back(
            std::make_tuple(decoded_dst_ids[i],
                            decoded_timestamps[i],
                            attrs.substr(i * edge_width, edge_width)));
    }
    return result;
}

// TODO
void SuccinctGraph::assoc_get(
    int64_t src,
    int32_t atype,
    std::set<int64_t> dst_id_set,
    int64_t t_low,
    int64_t t_high) {
}

// TODO
void SuccinctGraph::assoc_count(int64_t src, int32_t atype) {
}

// TODO
void SuccinctGraph::assoc_time_range(
    int64_t src,
    int32_t atype,
    int64_t t_low,
    int64_t t_high,
    int32_t len) {
}

std::string SuccinctGraph::succinct_directory() {
    return this->succinct_dir;
}

int64_t SuccinctGraph::num_nodes() {
    return this->node_table->num_keys() - 1; // FIXME: a bug in SuccinctShard
}

int64_t SuccinctGraph::num_edges() {
    return edges;
}

int64_t SuccinctGraph::num_attributes() {
    return NUM_ATTRIBUTES;
}

size_t SuccinctGraph::storage_size() {
}

size_t SuccinctGraph::serialize() {
}

/******* Old API *******/
void SuccinctGraph:: get_attribute(std::string& result, int64_t node_id, int attr) { }

void SuccinctGraph:: get_neighbors(std::vector<int64_t>& result, int64_t key) { }
void SuccinctGraph:: get_neighbors_of_node(std::vector<int64_t>& result, int64_t node_id,
    int attr, std::string search_key) { }

void SuccinctGraph:: search_nodes(std::set<int64_t>& result, int attr, std::string search_key) { }
void SuccinctGraph:: search_nodes(std::set<int64_t>& result, int attr1, std::string search_key1,
                                             int attr2, std::string search_key2) { }
