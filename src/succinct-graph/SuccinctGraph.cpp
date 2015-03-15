#include "../../include/succinct-graph/SuccinctGraph.hpp"
#include <iostream>

SuccinctGraph::SuccinctGraph(std::string datafile, bool construct) {
    this->input_datafile = datafile;
    this->nodes = 0;
    this->edges = 0;
    if (construct) {
        datafile = format_data_file(datafile);
    }
    //TODO: generalize id so we can create multiple succinct graphs
    this->shard = new SuccinctShard(0, datafile, construct);
}

int64_t SuccinctGraph::num_nodes() {
    return nodes;
}

int64_t SuccinctGraph::num_edges() {
    return edges;
}

void SuccinctGraph::get_neighbors(std::string& result, int64_t key) {
    this->shard->get(result, key);
}

std::string SuccinctGraph::format_data_file(std::string datafile) {
    std::ifstream input(datafile);
    std::vector< std::list<int> > neighbor_list;

    for(this->edges = 0; !input.eof(); this->edges++) {
        std::string line;
        std::getline(input, line, '\n');
        int split_pos = line.find(' ');
        if (split_pos == -1)
            break;
        int from_node = std::atoi(line.substr(0, split_pos).c_str());
        int to_node = std::atoi(line.substr(split_pos + 1).c_str());
        int bigger_id = std::max(from_node, to_node);
        if (bigger_id >= this->nodes) {
            this->nodes = bigger_id + 1;
            neighbor_list.resize(this->nodes, std::list<int>());
        }

        neighbor_list[from_node].push_back(to_node);
    }
    input.close();

    std::string tempfile = datafile + ".graph";
    std::ofstream s_out(tempfile);
    for (int node = 0; node < this->nodes; node++) {
        std::list<int> neighbors = neighbor_list[node];
        neighbors.sort();
        for (int n: neighbors) {
            s_out << " " + std::to_string(n);
        }
        s_out << "\n";
    }
    s_out.close();
    return tempfile;
}

size_t SuccinctGraph::serialize(std::ostream& out) {
    return shard->serialize(out);
}
