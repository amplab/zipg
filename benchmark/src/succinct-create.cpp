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

// Generates `num_names` random strings of length `len`, where each string
// appears `freq` number of times, except possibly the last.
std::vector<std::string> create_names(int num_names, int freq, int len) {
    std::string name = std::string(len, '0');
    std::vector<std::string> names;
    while (num_names > 0) {
        generate_name(name, len);
        int i = 0;
        while (num_names > 0 && i < freq) {
            names.push_back(name);
            num_names--;
            i++;
        }
    }
    std::random_shuffle(names.begin(), names.end());
    return names;
}

// Generates random attributes for all nodes.
void create_node_names(int nodes, int num_attr, int freq, int len, const std::string& node_file) {
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

// Format: [delim-separated attrs], <space>, [space-separated sorted nbhr ids].
void create_graph_file(std::string node_file, std::string edge_file, std::string graph_file) {
    std::ifstream node_input(node_file);
    std::ifstream edge_input(edge_file);
    std::vector<std::string> node_names; // each string = all attributes for a node
    std::string line;

    int nodes;
    for(nodes = 0; !node_input.eof(); nodes++) {
        std::getline(node_input, line, '\n');
        if (line.length() == 0)
            break;
        line = ',' + line; //prepend each data element with a comma
        int pos = -1;
        // replace commas, e.g. ",attr1,attr2,attr3" -> "∆attr1$attr2*att3"
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

void create_succinct_file(std::string graph_file, int sa_sr, int isa_sr, int npa_sr) {
    SuccinctGraph * graph = new SuccinctGraph(graph_file, true, sa_sr, isa_sr, npa_sr);
    graph->serialize();
}

// Format: randomNodeId.
void generate_neighbor_queries(int64_t nodes, int warmup_size, int query_size, std::string warmup_query_file, std::string query_file) {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni(0, nodes - 1);

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

// Format: attrIdx1,attrKey1,attrIdx2,attrKey2.
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

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for(int64_t i = 0; i < warmup_size; i++) {
        int node_id = uni_node(rng);
        int attr1 = uni_attr(rng);
        std::string search_key1 = attributes->at(node_id)->at(attr1);
        int attr2 = uni_attr(rng);
        std::string search_key2 = attributes->at(node_id)->at(attr2);
        warmup_out << attr1 << "," << search_key1 << "," << attr2 << "," << search_key2 << "\n";
    }

    for(int64_t i = 0; i < query_size; i++) {
        int node_id = uni_node(rng);
        int attr1 = uni_attr(rng);
        std::string search_key1 = attributes->at(node_id)->at(attr1);
        int attr2 = uni_attr(rng);
        std::string search_key2 = attributes->at(node_id)->at(attr2);
        query_out << attr1 << "," << search_key1 << "," << attr2 << "," << search_key2 << "\n";
    }
    warmup_out.close();
    query_out.close();
}

// Format: randomNodeId,attrIdx,attrKey.
void generate_neighbor_node_queries(
    std::string node_succinct_dir,
    std::string edge_succinct_dir,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file) {

    SuccinctGraph* graph =
        new SuccinctGraph(node_succinct_dir, edge_succinct_dir);

    std::random_device rd;
    std::mt19937 rng(rd());

    // TODO: -1 twice?
    std::uniform_int_distribution<int64_t> uni_node(0, graph->num_nodes() - 1);
    std::uniform_int_distribution<int> uni_attr(0, graph->num_attributes() - 1);

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for(int64_t i = 0; i < warmup_size; i++) {
        int node_id = uni_node(rng);
        int attr = uni_attr(rng);
        std::vector<int64_t> neighbors;
        graph->get_neighbors(neighbors, node_id);
        std::vector<int64_t>::const_iterator it(neighbors.begin());
        int neighbor_idx = rand() % neighbors.size();
        std::advance(it, neighbor_idx);
        std::string search_key;
        graph->get_attribute(search_key, *it, attr);
        warmup_out << node_id << "," << attr << "," << search_key << "\n";
    }

    for(int64_t i = 0; i < query_size; i++) {
        int node_id = uni_node(rng);
        int attr = uni_attr(rng);
        std::vector<int64_t> neighbors;
        graph->get_neighbors(neighbors, node_id);
        std::vector<int64_t>::const_iterator it(neighbors.begin());
        int neighbor_idx = rand() % neighbors.size();
        std::advance(it, neighbor_idx);
        std::string search_key;
        graph->get_attribute(search_key, *it, attr);
        query_out << node_id << "," << attr << "," << search_key << "\n";
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
        std::string node_file = std::to_string(nodes) + ".node";
        if (argc > 6) node_file = argv[6];
        create_node_names(nodes, attributes, freq, len, node_file);
    } else if (type == "graph") {
        std::string node_file = argv[2];
        std::string edge_file = argv[3];
        std::string graph_file = node_file.substr(0, node_file.find(".node")) + ".graph";
        create_graph_file(node_file, edge_file, graph_file);
    } else if (type == "succinct") {
        std::string graph_file = argv[2];
        int sa_sr = 32, isa_sr = 32, npa_sr = 128;
        if (argc >= 6) {
            sa_sr = std::stoi(argv[3]);
            isa_sr = std::stoi(argv[4]);
            npa_sr = std::stoi(argv[5]);
        }
        create_succinct_file(graph_file, sa_sr, isa_sr, npa_sr);
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
    } else if (type == "neighbor-node-queries"){
        std::string node_succinct_dir = argv[2];
        std::string edge_succinct_dir = argv[3];
        int warmup_size = atoi(argv[4]);
        int query_size = atoi(argv[5]);
        std::string warmup_file = argv[6];
        std::string query_file = argv[7];
        generate_neighbor_node_queries(
            node_succinct_dir, edge_succinct_dir,
            warmup_size, query_size, warmup_file, query_file);
    } else {
        assert(1); // not supported
    }
    return 0;
}
