#include "succinct-graph/SuccinctGraph.hpp"
#include <limits>
#include <sstream>

// TODO: make ATTR_SIZE a parameter to be passed in
const int ATTR_SIZE = 32;
const int NUM_ATTRIBUTES = 10;
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

SuccinctGraph& SuccinctGraph::build(
    std::string node_file,
    std::string edge_file,
    bool construct) {

    if (construct) {
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
    } else {
    }
    return *this;
}

void SuccinctGraph::obj_get(std::string& result, int64_t obj_id) {
    this->node_table->get(result, obj_id);
}

std::string SuccinctGraph::succinct_directory() {
    return this->succinct_dir;
}

int64_t SuccinctGraph::num_nodes() {
    return this->node_table->num_keys();
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
