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

std::string SuccinctGraph::format_data_file(std::string datafile) {
    std::ifstream input(datafile);

    std::unordered_map <int, int> name_to_id;
    for(this->edges = 0; !input.eof(); this->edges++) {
        std::string line;
        std::getline(input, line, '\n');
        int split_pos = line.find(' ');
        int from_node = atoi(line.substr(0, split_pos).c_str());
        int to_node = atoi(line.substr(split_pos + 1).c_str());
        printf("%d, %d", from_node, to_node);
    }
    input.close();

    std::string tempfile = datafile + ".succinct.graph";

    std::ofstream s_out(tempfile);


    return tempfile;
}
