#include "succinct-graph/SuccinctGraph.hpp"
#include <iostream>
#include <limits>
#include <sstream>

const int ATTR_SIZE = 32;
const std::string DELIMINATORS = "<>()#$%&*+";

SuccinctGraph::SuccinctGraph(std::string node_file, std::string edge_file) {
    this->nodes = 0;
    this->edges = 0;
    this->graph_file = format_input_data(node_file, edge_file);
    //TODO: generalize id so we can create multiple succinct graphs
    this->shard = new SuccinctShard(0, this->graph_file, SuccinctMode::CONSTRUCT_IN_MEMORY);
}

SuccinctGraph::SuccinctGraph(std::string graph_file) {
    this->graph_file = graph_file;
    //TODO: also find a way of computing edges when we do not construct
    this->nodes = lines_in_file(graph_file);
    this->shard = new SuccinctShard(0, graph_file, SuccinctMode::LOAD_MEMORY_MAPPED);
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

void SuccinctGraph::get_neighbors(std::string& result, int64_t key) {
    // std::string line;
    this->shard->access(result, key, this->num_attributes() * (ATTR_SIZE + 1) + 1, std::numeric_limits<int32_t>::max());
    //std::istringstream iss(line);
    //std::string token;
    // TOOO: make edge deliminators not necessarily blank space
    //while (getline(iss, token, ' ')) {
    //    result.insert(std::strtoll(token.c_str(), NULL, 10));
    //}
}

std::string SuccinctGraph::format_input_data(std::string node_file, std::string edge_file) {
    std::ifstream node_input(node_file);
    std::ifstream edge_input(edge_file);
    std::vector<std::string> node_names;
    std::string line;

    for(this->nodes = 0; !node_input.eof(); this->nodes++) {
        std::getline(node_input, line, '\n');
        if (line.length() == 0)
            break;
        line = ',' + line; //prepend each data element with a comma
        int pos = -1;
        for (char delim: DELIMINATORS) {
            pos = line.find(',', pos + 1);
            line[pos] = delim;
        }
        node_names.push_back(line);
    }

    std::vector< std::list<int> > neighbor_list(this->nodes);
    for(this->edges = 0; !edge_input.eof(); this->edges++) {
        std::getline(edge_input, line, '\n');
        if (line.length() == 0)
            break;
        int split_pos = line.find(' ');
        int from_node = std::atoi(line.substr(0, split_pos).c_str());
        int to_node = std::atoi(line.substr(split_pos + 1).c_str());
        neighbor_list[from_node].push_back(to_node);
    }
    node_input.close();
    edge_input.close();

    std::string graph_file = node_file.substr(0, node_file.find(".node")) + ".graph";
    std::ofstream s_out(graph_file);
    for (int node = 0; node < this->nodes; node++) {
        std::list<int> neighbors = neighbor_list[node];
        neighbors.sort();
        s_out << node_names[node];
        for (int n: neighbors) {
            s_out << " " << n;
        }
        s_out << "\n";
    }
    s_out.close();
    return graph_file;
}

size_t SuccinctGraph::serialize() {
    return shard->serialize();
}

size_t SuccinctGraph::lines_in_file(std::string file_path) {
    std::ifstream file(file_path);
    size_t lines = 0;
    std::string line;
    while (std::getline(file, line)) {
        lines++;
    }
    return lines;
}
