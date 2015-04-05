#include <iostream>
#include <fstream>
#include <unistd.h>
#include "../../include/succinct-graph/SuccinctGraph.hpp"

constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

void generate_name(std::string& name) {
    for(int i = 0; i < 100; i++) {
        name[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

void create_node_names(int nodes, int freq) {
    std::string node_file = std::to_string(nodes) + "_" + std::to_string(freq) + ".node";
    std::ofstream s_out(node_file);
    std::string name = std::string(100, '0');
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
    for(std::string name: names) {
        s_out << name << std::endl;
    }
    s_out.close();
    printf("Created node file: %s\n", node_file.c_str());
}

void create_succinct_file(std::string node_file, std::string edge_file) {
    SuccinctGraph * graph = new SuccinctGraph(node_file, edge_file);
    std::string succinct_file = node_file.substr(0, node_file.find('.')) + ".graph.succinct";
    // Serialize and save to file
    std::ofstream s_out(succinct_file);
    graph->serialize(s_out);
    s_out.close();
    printf("Created succinct graph: %s\n", succinct_file.c_str());
}

int main(int argc, char **argv) {
    std::string type = argv[1];
    if (type == "nodes") {
        int nodes = atoi(argv[2]);
        int freq = atoi(argv[3]);
        create_node_names(nodes, freq);
    } else if (type == "succinct") {
        std::string node_file = argv[2];
        std::string edge_file = argv[3];
        create_succinct_file(node_file, edge_file);
    } else {
        assert(1); // not supported
    }
    return 0;
}
