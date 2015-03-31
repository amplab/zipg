#include "../../include/succinct-graph/SuccinctGraph.hpp"
#include <iostream>

const int SuccinctGraph::NAME_SIZE = 4;
const std::string SuccinctGraph::NAME_DELIMINATOR = "<";

SuccinctGraph::SuccinctGraph(std::string node_file, std::string edge_file) {
    this->nodes = 0;
    this->edges = 0;
    this->graph_file = format_input_data(node_file, edge_file);
    //TODO: generalize id so we can create multiple succinct graphs
    this->shard = new SuccinctShard(0, this->graph_file);
}

SuccinctGraph::SuccinctGraph(std::string graph_file) {
    this->graph_file = graph_file;
    //TODO: also find a way of computing edges when we do not construct
    this->nodes = lines_in_file(graph_file);
    this->shard = new SuccinctShard(0, graph_file, false);
}

int64_t SuccinctGraph::num_nodes() {
    return nodes;
}

int64_t SuccinctGraph::num_edges() {
    return edges;
}

void SuccinctGraph::get_name(std::string& result, int64_t key) {
    return this->shard->access(result, key, NAME_DELIMINATOR.length(), NAME_SIZE);
}

void SuccinctGraph::get_nodes(std::set<int64_t>& result, std::string name) {
    this->shard->search(result, NAME_DELIMINATOR + name);
}

void SuccinctGraph::get_neighbors(std::string& result, int64_t key) {
    this->shard->get(result, key);
}

std::string SuccinctGraph::format_input_data(std::string node_file, std::string edge_file) {
    std::ifstream node_input(node_file);
    std::ifstream edge_input(edge_file);
    std::vector< std::string > node_names;
    std::string line;

    for(this->nodes = 0; !node_input.eof(); this->nodes++) {
        std::getline(node_input, line, '\n');
        if (line.length() == 0)
            break;
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

    std::string graph_file = node_file.substr(0, node_file.find('.')) + ".graph";
    std::ofstream s_out(graph_file);
    for (int node = 0; node < this->nodes; node++) {
        std::list<int> neighbors = neighbor_list[node];
        neighbors.sort();
        s_out << NAME_DELIMINATOR << node_names[node];
        for (int n: neighbors) {
            s_out << " " + std::to_string(n);
        }
        s_out << "\n";
    }
    s_out.close();
    return graph_file;
}

size_t SuccinctGraph::serialize(std::ostream& out) {
    return shard->serialize(out);
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
