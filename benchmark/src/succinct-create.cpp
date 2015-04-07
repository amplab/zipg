#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include "succinct-graph/SuccinctGraph.hpp"

constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

void generate_name(std::string& name) {
    for(int i = 0; i < 10; i++) {
        name[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

std::vector<std::string> create_names(int nodes, int freq) {
    std::string name = std::string(10, '0');
    std::vector<std::string> names;
    while (nodes > 0) {
        generate_name(name);
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

void create_node_names(int nodes, int num_attr, int freq) {
    std::string node_file = std::to_string(nodes) + "_" + std::to_string(freq) + ".node";
    std::ofstream s_out(node_file);
    std::vector<std::vector<std::string>> attributes;
    for (int attr = 0; attr < num_attr; attr++ ) { 
        attributes.push_back(create_names(nodes, freq));
    }
    for (int i = 0; i < nodes; i++) {
        s_out << attributes[0][i];
        for (int attr = 1; attr < num_attr; attr++ ) { 
            s_out << "," << attributes[attr][i];
        }
        s_out << std::endl;
    }
    s_out.close();
    printf("Created node file: %s\n", node_file.c_str());
}

void create_succinct_file(std::string node_file, std::string edge_file) {
    SuccinctGraph * graph = new SuccinctGraph(node_file, edge_file);
    std::string succinct_file = node_file.substr(0, node_file.find(".node")) + ".graph.succinct";
    // Serialize and save to file
    std::ofstream s_out(succinct_file);
    graph->serialize(s_out);
    s_out.close();
    printf("Created succinct graph: %s\n", succinct_file.c_str());
}

void generate_query_files(std::string node_file, std::string warmup_query_file, std::string query_file) {
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
    std::uniform_int_distribution<int64_t> uni(0, nodes - 1);
    std::string value;

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for(int64_t i = 0; i < nodes; i++) {
        int attr = rand() % num_attributes;
        value = attributes->at(uni(rng))->at(attr); 
        warmup_out << attr << "," << value << std::endl;
    }

    for(int64_t i = 0; i < 2 * nodes; i++) {
        int attr = rand() % num_attributes;
        value = attributes->at(uni(rng))->at(attr); 
        query_out << attr << "," << value << std::endl;
    }
    warmup_out.close();
    query_out.close();
    printf("Created files: %s, %s\n", warmup_query_file.c_str(), query_file.c_str());
}

int main(int argc, char **argv) {
    std::string type = argv[1];
    if (type == "nodes") {
        int nodes = atoi(argv[2]);
        int attributes = atoi(argv[3]);
        int freq = atoi(argv[4]);
        create_node_names(nodes, attributes, freq);
    } else if (type == "succinct") {
        std::string node_file = argv[2];
        std::string edge_file = argv[3];
        create_succinct_file(node_file, edge_file);
    } else if (type == "queries") {
        std::string node_file = argv[2];
        std::string warmup_file = argv[3];
        std::string query_file = argv[4];
        generate_query_files(node_file, warmup_file, query_file);
    } else {
        assert(1); // not supported
    }
    return 0;
}
