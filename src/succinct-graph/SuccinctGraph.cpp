#include "succinct-graph/SuccinctGraph.hpp"
#include <limits>
#include <sstream>

//TODO: make ATTR_SIZE a parameter to be passed in
const int ATTR_SIZE = 32;
const std::string SuccinctGraph::DELIMINATORS = "<>()#$%&*+[]{}^-|~;? \"',./:=@\\_~\x02\x03\x04\x05\x06\x07\x08\x09";

SuccinctGraph::SuccinctGraph(std::string file, bool construct) {
    if (construct) {
        //TODO: generalize id so we can create multiple succinct graphs
        this->shard = new SuccinctShard(0, file, SuccinctMode::CONSTRUCT_IN_MEMORY);
    } else {
        this->succinct_dir = file;
        this->shard = new SuccinctShard(0, this->succinct_dir, SuccinctMode::LOAD_MEMORY_MAPPED);
    }
    //TODO: also find a way of computing edges when we do not construct
    this->nodes = this->shard->num_keys();
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
    return DELIMINATORS.size();
}

void SuccinctGraph::get_attribute(std::string& result, int64_t key, int attr) {
    return this->shard->access(result, key, attr * (ATTR_SIZE + 1) + 1, ATTR_SIZE);
}

void SuccinctGraph::search_nodes(std::set<int64_t>& result, int attr, std::string search_key) {
    this->shard->search(result, DELIMINATORS[attr] + search_key);
}

void SuccinctGraph::get_neighbors(std::set<int64_t>& result, int64_t key) {
    std::string line;
    this->shard->access(line, key, this->num_attributes() * (ATTR_SIZE + 1) + 1, std::numeric_limits<int32_t>::max());
    std::istringstream iss(line);
    std::string token;
    // TOOO: make edge deliminators not necessarily blank space
    while (getline(iss, token, ' ')) {
        result.insert(std::strtoll(token.c_str(), NULL, 10));
    }
}

size_t SuccinctGraph::storage_size() {
    return shard->storage_size();
}

size_t SuccinctGraph::serialize() {
    return shard->serialize();
}

