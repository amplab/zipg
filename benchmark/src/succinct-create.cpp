#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <random>
#include <vector>
#include <list>

#include "succinct-graph/SuccinctGraph.hpp"

constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

void generate_name(std::string& name, int len) {
    for(int i = 0; i < len; i++) {
        name[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

std::vector<std::string> create_names(int nodes, int freq, int len) {
    std::string name = std::string(len, '0');
    std::vector<std::string> names;
    while (nodes > 0) {
        generate_name(name, len);
        int i = 0;
        while (nodes > 0 && i < freq) {
            names.push_back(name);
            nodes--;
            i++;
        }
    }
    std::random_shuffle(names.begin(), names.end());
    return names;
}

void create_node_names(int nodes, int num_attr, int freq, int len) {
    std::string node_file = std::to_string(nodes) + ".node";
    std::ofstream s_out(node_file);
    std::vector<std::vector<std::string>> attributes;
    for (int attr = 0; attr < num_attr; attr++ ) {
        attributes.push_back(create_names(nodes, freq, len));
    }
    for (int i = 0; i < nodes; i++) {
        s_out << attributes[0][i];
        for (int attr = 1; attr < num_attr; attr++ ) {
            s_out << "," << attributes[attr][i];
        }
        s_out << std::endl;
    }
    s_out.close();
}

void create_graph_file(std::string node_file, std::string edge_file, std::string graph_file) {
    std::ifstream node_input(node_file);
    std::ifstream edge_input(edge_file);
    std::vector<std::string> node_names;
    std::string line;

    int nodes;
    for(nodes = 0; !node_input.eof(); nodes++) {
        std::getline(node_input, line, '\n');
        if (line.length() == 0)
            break;
        line = ',' + line; //prepend each data element with a comma
        int pos = -1;
        for (char delim: SuccinctGraph::DELIMINATORS) {
            pos = line.find(',', pos + 1);
            line[pos] = delim;
        }
        node_names.push_back(line);
    }

    std::vector< std::list<int> > neighbor_list(nodes);
    for(int edges = 0; !edge_input.eof(); edges++) {
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

    std::ofstream s_out(graph_file);
    for (int node = 0; node < nodes; node++) {
        std::list<int> neighbors = neighbor_list[node];
        neighbors.sort();
        s_out << node_names[node];
        for (int n: neighbors) {
            s_out << " " << n;
        }
        s_out << "\n";
    }
    s_out.close();
}

void create_succinct_file(std::string graph_file) {
    SuccinctGraph * graph = new SuccinctGraph(graph_file, true);
    graph->serialize();
}

void generate_neighbor_queries(int64_t nodes, int warmup_size, int query_size, std::string warmup_query_file, std::string query_file) {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni(0, nodes);

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for(int64_t i = 0; i < warmup_size; i++) {
        warmup_out << uni(rng) << std::endl;
    }

    for(int64_t i = 0; i < query_size; i++) {
        query_out << uni(rng) << std::endl;
    }
    warmup_out.close();
    query_out.close();
}

void generate_node_queries(std::string node_file, int warmup_size, int query_size, std::string warmup_query_file, std::string query_file) {
    int64_t nodes = 0;
    std::ifstream node_input(node_file);
    std::string line;
    std::vector<std::vector<std::string>*>* attributes = new std::vector<std::vector<std::string>*>();
    while (getline(node_input, line)) {
        nodes++;
        std::vector<std::string>* attr = new std::vector<std::string>();
        std::istringstream iss(line);
        std::string token;
        while (getline(iss, token, ',')) {
            attr->push_back(token);
        }
        attributes->push_back(attr);
    }

    size_t num_attributes = attributes->at(0)->size();
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, nodes - 1);
    std::uniform_int_distribution<int> uni_attr(0, num_attributes - 1);
    std::string value;

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for(int64_t i = 0; i < warmup_size; i++) {
        int attr = uni_attr(rng);
        value = attributes->at(uni_node(rng))->at(attr);
        warmup_out << attr << "," << value << std::endl;
    }

    for(int64_t i = 0; i < query_size; i++) {
        int attr = uni_attr(rng);
        value = attributes->at(uni_node(rng))->at(attr);
        query_out << attr << "," << value << std::endl;
    }
    warmup_out.close();
    query_out.close();
}

int main(int argc, char **argv) {
    std::string type = argv[1];
    if (type == "nodes") {
        int nodes = atoi(argv[2]);
        int attributes = atoi(argv[3]);
        int freq = atoi(argv[4]);
        int len = atoi(argv[5]);
        create_node_names(nodes, attributes, freq, len);
    } else if (type == "graph") {
        std::string node_file = argv[2];
        std::string edge_file = argv[3];
        std::string graph_file = node_file.substr(0, node_file.find(".node")) + ".graph";
        create_graph_file(node_file, edge_file, graph_file);
    } else if (type == "succinct") {
        std::string graph_file = argv[2];
        create_succinct_file(graph_file);
    } else if (type == "node-queries") {
        std::string node_file = argv[2];
        int warmup_size = atoi(argv[3]);
        int query_size = atoi(argv[4]);
        std::string warmup_file = argv[5];
        std::string query_file = argv[6];
        generate_node_queries(node_file, warmup_size, query_size, warmup_file, query_file);
    } else if (type == "neighbor-queries") {
        int nodes = atoi(argv[2]);
        int warmup_size = atoi(argv[3]);
        int query_size = atoi(argv[4]);
        std::string warmup_file = argv[5];
        std::string query_file = argv[6];
        generate_neighbor_queries(nodes, warmup_size, query_size, warmup_file, query_file);
    } else {
        assert(1); // not supported
    }
    return 0;
}
