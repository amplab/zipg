#include <iostream>
#include <fstream>
#include <unistd.h>
#include "../../include/succinct-graph/SuccinctGraph.hpp"

constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

void generate_name(std::string& name) {
    for(int i = 0; i < SuccinctGraph::NAME_SIZE; i++) {
        name[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

void create_node_names(std::string node_file_name, int nodes, int freq) {
    std::ofstream s_out(node_file_name + "_" + std::to_string(freq) + ".node");
    std::string name = "0000";
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
}

int main(int argc, char **argv) {
    std::string name = argv[1];
    int nodes = atoi(argv[2]);
    for (int i = 3; i < argc; i++) {
        int freq = atoi(argv[i]);
        create_node_names(name, nodes, freq);
    }
    return 0;
}
