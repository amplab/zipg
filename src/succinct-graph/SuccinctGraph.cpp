#include "succinct-graph/SuccinctGraph.hpp"
#include <limits>
#include <sstream>

//TODO: make ATTR_SIZE a parameter to be passed in
const int ATTR_SIZE = 350;
const int NUM_ATTRIBUTES = 2;

// Used in node table layout only.  Prefer the \x** weird characters first.
const std::string SuccinctGraph::DELIMITERS =
    "\x02\x03\x04\x05\x06\x07\x08\x0C\x0D\x0E\x0F\x10\x11\x12\x13\x14\x15"
    "<>()#$%&*+[]{}^-;? \"',./:=@|\\_~";

// Hard assumption: support up to this many # of node attributes.  The character
// in DELIMITERS indexed by this is used as a special end-of-record delim
// appended to every value in node table.
const int SuccinctGraph::MAX_NUM_NODE_ATTRS = 16;


SuccinctGraph::SuccinctGraph(std::string file, bool construct,
    uint32_t sa_sampling_rate,
    uint32_t isa_sampling_rate,
    uint32_t npa_sampling_rate) {

    if (construct) {
        //TODO: generalize id so we can create multiple succinct graphs
        this->shard = new SuccinctShard(0, file, SuccinctMode::CONSTRUCT_IN_MEMORY,
            sa_sampling_rate, isa_sampling_rate, npa_sampling_rate);
    } else {
        this->succinct_dir = file;
        this->shard = new SuccinctShard(0, this->succinct_dir, SuccinctMode::LOAD_MEMORY_MAPPED);
    }
    //TODO: also find a way of computing edges when we do not construct
    //FIXME: this seems to return +1 higher than the real value
    this->nodes = this->shard->num_keys() - 1;
}

std::string SuccinctGraph::succinct_directory() {
    return this->succinct_dir;
}

int64_t SuccinctGraph::num_nodes() {
    return nodes;
}

int64_t SuccinctGraph::num_edges() {
    return edges;
}

int64_t SuccinctGraph::num_attributes() {
    return NUM_ATTRIBUTES;
}

// Same as new format.
inline std::string mk_node_attr_key(int attr, const std::string& query_key) {
    assert(attr < SuccinctGraph::MAX_NUM_NODE_ATTRS);
    return SuccinctGraph::DELIMITERS[attr] +
        query_key +
        SuccinctGraph::DELIMITERS[attr + 1];
}

// Due to fixed width assumption, extract everything, then pick out nhbrs.
// This is the only primitive query whose impl differs from new format.
void SuccinctGraph::get_neighbors(std::vector<int64_t>& result, int64_t node_id) {
    result.clear();
    std::string line;
    this->shard->access(line, node_id, 0, std::numeric_limits<int32_t>::max());
    auto split_point = line.find(DELIMITERS[MAX_NUM_NODE_ATTRS]);
    assert(split_point != std::string::npos);
    std::istringstream iss(line.substr(split_point + 1));
    std::string token;
    while (getline(iss, token, ' ')) {
        result.push_back(std::strtoll(token.c_str(), NULL, 10));
    }
}

// Same as new format.
void SuccinctGraph::search_nodes(
    std::set<int64_t>& result,
    int attr,
    const std::string& search_key) {

    result.clear();
    this->shard->search(result, mk_node_attr_key(attr, search_key));
}

// Same as new format.
void SuccinctGraph::search_nodes(
    std::set<int64_t>& result,
    int attr1,
    const std::string& search_key1,
    int attr2,
    const std::string& search_key2) {

    result.clear();
    std::set<int64_t> s1, s2;
    this->shard->search(s1, mk_node_attr_key(attr1, search_key1));
    this->shard->search(s2, mk_node_attr_key(attr2, search_key2));
    // result.end() is a hint that supposedly is faster than .begin()
    std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                          std::inserter(result, result.end()));
}

// FIXME: deprecated & incorrect. Left here so that we can compile.
void SuccinctGraph::get_attribute(std::string& result, int64_t node_id, int attr) {
    return this->shard->access(result, node_id, attr * (ATTR_SIZE + 1) + 1, ATTR_SIZE);
}

// Iterative extract, same logic as new format!
void SuccinctGraph::get_neighbors_of_node(
    std::vector<int64_t>& result,
    int64_t node_id,
    int attr,
    const std::string& search_key) {

    result.clear();

    assert(attr < SuccinctGraph::MAX_NUM_NODE_ATTRS);
    char attr_delim = DELIMITERS[attr], next_attr_delim = DELIMITERS[attr + 1];

    int32_t off, idx, search_len = search_key.length();
    std::string singleton;

    std::vector<int64_t> nbhrs;
    get_neighbors(nbhrs, node_id);

    for (auto nhbrId : nbhrs) {
        off = 0;

        // extract first to see if nhbrId is absent in table
        this->shard->access(singleton, nhbrId, off, 1);
        if (singleton.empty()) continue;
        ++off;

        if (singleton[0] != attr_delim) {
            // iterative extract until hitting delim for attr
            while (true) {
                this->shard->access(singleton, nhbrId, off, 1);
                ++off;
                if (singleton[0] == attr_delim) break;
            }
        }

        // found start of attr column
        idx = 0;
        while (idx < search_len) {
            this->shard->access(singleton, nhbrId, off, 1);
            ++off;
            if (singleton[0] != search_key[idx]) break;
            ++idx;
        }
        if (idx < search_len) continue;

        // enforce exact match semantics
        this->shard->access(singleton, nhbrId, off, 1);
        if (singleton[0] == next_attr_delim) result.push_back(nhbrId);
    }
}

size_t SuccinctGraph::storage_size() {
    return shard->storage_size();
}

size_t SuccinctGraph::serialize() {
    return shard->serialize();
}

