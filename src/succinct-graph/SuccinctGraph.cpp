#include "succinct-graph/SuccinctGraph.hpp"
#include <limits>
#include <sstream>

//TODO: make ATTR_SIZE a parameter to be passed in
const int ATTR_SIZE = 350;
const int NUM_ATTRIBUTES = 2;
const std::string SuccinctGraph::DELIMINATORS = "<>()#$%&*+[]{}^-|~;? \"',./:=@\\_~\x02\x03\x04\x05\x06\x07\x08\x09";

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

// TODO: keep and exploit fixed width assumption for now.
void SuccinctGraph::get_neighbors(std::vector<int64_t>& result, int64_t node_id) {
    std::string line;
    this->shard->access(line, node_id, this->num_attributes() * (ATTR_SIZE + 1) + 1, std::numeric_limits<int32_t>::max());
    std::istringstream iss(line);
    std::string token;
    // TOOO: make edge deliminators not necessarily blank space
    while (getline(iss, token, ' ')) {
        result.push_back(std::strtoll(token.c_str(), NULL, 10));
    }
}

// TODO: keep and exploit fixed width assumption for now.
void SuccinctGraph::get_attribute(std::string& result, int64_t node_id, int attr) {
    return this->shard->access(result, node_id, attr * (ATTR_SIZE + 1) + 1, ATTR_SIZE);
}

// okay
void SuccinctGraph::search_nodes(
    std::set<int64_t>& result,
    int attr,
    const std::string& search_key) {

    result.clear();
    this->shard->search(result, DELIMINATORS[attr] + search_key);
}

// okay
void SuccinctGraph::search_nodes(
    std::set<int64_t>& result,
    int attr1,
    const std::string& search_key1,
    int attr2,
    const std::string& search_key2) {

    result.clear();
    std::set<int64_t> s1, s2;
    this->shard->search(s1, DELIMINATORS[attr1] + search_key1);
    this->shard->search(s2, DELIMINATORS[attr2] + search_key2);
    std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                          std::inserter(result, result.begin()));
}

// TODO: keep and exploit fixed width assumption for now.
void SuccinctGraph::get_neighbors_of_node(
    std::vector<int64_t>& result,
    int64_t node_id,
    int attr,
    const std::string& search_key) {

    std::vector<int64_t> nbhrs;
    this->get_neighbors(nbhrs, node_id);

    result.clear();
    std::string attribute;
    for (auto it = nbhrs.begin(); it != nbhrs.end(); ++it) {
        this->get_attribute(attribute, *it, attr);
        if (search_key.compare(attribute) == 0) {
            result.push_back(*it);
        }
    }
}

size_t SuccinctGraph::storage_size() {
    return shard->storage_size();
}

size_t SuccinctGraph::serialize() {
    return shard->serialize();
}

