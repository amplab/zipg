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

        fprintf(stderr, "Initializing edge table (SuccinctFile)\n");

        AssocMap assoc_map;

        std::ifstream edge_file_stream(edge_file);
        std::string line, token;
        NodeId src_id, dst_id;
        AType atype;
        Timestamp time;
        while (std::getline(edge_file_stream, line)) {
            std::stringstream ss(line);
            int token_idx = 0;
            while (std::getline(ss, token, ' ')) {
                ++token_idx;
                if (token_idx == 1) src_id = std::stoi(token);
                else if (token_idx == 2) dst_id = std::stoi(token);
                else if (token_idx == 3) atype = std::stoi(token);
                else if (token_idx == 4) time = std::stoi(token);
                else break;
                token.clear();
            }
            std::getline(ss, token); // rest of the data is attr
            Assoc assoc = { dst_id, time, token };
            assoc_map[std::make_pair(src_id, atype)].push_back(assoc);
        }

        for (AssocMapIt it = assoc_map.begin(); it != assoc_map.end(); ++it) {
            std::sort(it->second.begin(),
                      it->second.end(),
                      cmp_assoc_by_decreasing_time);
        }

        printf("\n");
        for (AssocMapIt it = assoc_map.begin(); it != assoc_map.end(); ++it) {
            std::vector<Assoc> assocs = it->second;
            printf("[node %lld, atype %d]: ",
                (it->first).first, (it->first).second);
            for (auto it2 = assocs.begin(); it2 != assocs.end(); ++it2) {
                printf(" (dst %lld, time %lld, attr %s)",
                       it2->dst_id,
                       it2->time,
                       (it2->attr).c_str());
            }
            printf("\n");
        }
        printf("\n");

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
    return this->node_table->num_keys() - 1; // FIXME: what's the last key?
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
