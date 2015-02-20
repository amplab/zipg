#include "../../include/succinct-graph/SuccinctGraph.hpp"

SuccinctGraph::SuccinctGraph(std::string datafile) {
    this->input_datafile = datafile;
    this->nodes = 0;
    this->edges = 0;
    std::string tempfile = format_data_file(datafile);
    this->shard = new SuccinctShard(0, tempfile);
}

size_t SuccinctGraph::num_nodes() {
    return nodes;
}

void SuccinctGraph::get_neighbors(std::string& result, int64_t key) {
    this->shard->get(result, key);
}

std::string SuccinctGraph::format_data_file(std::string datafile) {
    std::ifstream input(datafile);

    std::unordered_map <int, int> name_to_id;
    std::vector< std::list<int> > neighbor_list;
    std::unordered_map<int,int>::iterator it;
    for(this->edges = 0; !input.eof(); this->edges++) {
        std::string line;
        std::getline(input, line, '\n');
        int split_pos = line.find(' ');
        int from_node = atoi(line.substr(0, split_pos).c_str());
        int to_node = atoi(line.substr(split_pos + 1).c_str());

        it = name_to_id.find(from_node);
        if (it == name_to_id.end()) {
            name_to_id[from_node] = this->nodes;
            neighbor_list.push_back(std::list<int>());
            this->nodes++;
        }
        it = name_to_id.find(to_node);
        if (it == name_to_id.end()) {
            name_to_id[to_node] = this->nodes;
            neighbor_list.push_back(std::list<int>());
            this->nodes++;
        }

        int from_id = name_to_id[from_node];
        int to_id = name_to_id[to_node];
        neighbor_list[from_id].push_back(to_id);
    }
    input.close();

    std::string tempfile = datafile + ".succinct.graph.temp";
    std::ofstream s_out(tempfile);
    for (int node = 0; node < this->nodes; node++) {
        std::list<int> neighbors = neighbor_list[node];
        s_out << std::to_string(node) + " ->";
        for (int n: neighbors) {
            s_out << " " + std::to_string(n);
        }
        s_out << "\n";
    }
    s_out.close();
    return tempfile;
}
