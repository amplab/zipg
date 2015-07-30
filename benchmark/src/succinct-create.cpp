#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <random>
#include <thread>
#include <vector>
#include <list>

#include "succinct-graph/GraphFormatter.hpp"
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
// If the node attributes are not already uniquely delimited, assumes they
// are comma-separated and properly delim them.
void create_graph_file(
    std::string node_file,
    std::string edge_file,
    std::string graph_file,
    bool already_uniquely_delimed = false) {

    std::ifstream node_input(node_file);
    std::ifstream edge_input(edge_file);
    // each string = all attributes for a node
    std::vector<std::string> node_names;
    std::string line;

    int nodes;
    for (nodes = 0; !node_input.eof(); nodes++) {
        std::getline(node_input, line, '\n');
        if (line.length() == 0) break;
        if (!already_uniquely_delimed) {
            line = ',' + line; // prepend each data element with a comma
            int pos = -1;
            // replace commas, e.g. ",attr1,attr2,attr3" -> "∆attr1$attr2*att3"
            for (unsigned char delim : SuccinctGraph::DELIMITERS) {
                pos = line.find(',', pos + 1);
                line[pos] = static_cast<char>(delim);
            }
        }
        node_names.push_back(line);
    }

    std::vector< std::list<int> > neighbor_list(nodes);
    for(int edges = 0; !edge_input.eof(); edges++) {
        std::getline(edge_input, line, '\n');
        if (line.length() == 0) break;
        int split_pos = line.find(' ');
        int from_node = std::atoi(line.substr(0, split_pos).c_str());
        int next_split_pos = line.find(' ', split_pos + 1);
        int to_node;
        if (next_split_pos != std::string::npos) {
            // ignore rest of the fields, just take the dst node field
            to_node = std::atoi(line.substr(
                split_pos + 1, next_split_pos - split_pos - 1).c_str());
        } else {
            // there are no other fields, take to the end of line
            to_node = std::atoi(line.substr(split_pos + 1).c_str());
        }
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
void generate_neighbor_queries(
    int64_t num_nodes,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file)
{
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni(0, num_nodes - 1);

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for (int64_t i = 0; i < warmup_size; i++) {
        warmup_out << uni(rng) << std::endl;
    }
    for (int64_t i = 0; i < query_size; i++) {
        query_out << uni(rng) << std::endl;
    }
}

// Format: randomNodeId,atype.
void generate_neighbor_atype_queries(
    int64_t num_nodes,
    int max_num_atype,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file)
{
    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> uni1(0, num_nodes - 1);
    std::uniform_int_distribution<int64_t> uni2(0, max_num_atype - 1);

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for (int64_t i = 0; i < warmup_size; i++) {
        warmup_out << uni1(rng1) << "," << uni2(rng2) << std::endl;
    }

    for (int64_t i = 0; i < query_size; i++) {
        query_out << uni1(rng1) << "," << uni2(rng2) << std::endl;
    }
}

void read_node_attributes(
    std::vector<std::vector<std::string>>& attributes,
    int64_t& nodes,
    const std::string& node_attr_file,
    int num_actual_delims,
    bool is_comma_separated)
{
    std::ifstream node_input(node_attr_file);
    std::string line;
    while (std::getline(node_input, line)) {
        ++nodes;
        std::vector<std::string> attr;
        std::istringstream iss(line);
        std::string token;

        if (is_comma_separated) {
            while (std::getline(iss, token, ',')) {
                attr.push_back(token);
            }
        } else {
            // case: SuccinctGraph::DELIMITERS-separated

            // skip all the way until the delim before first attr
            std::getline(
                iss, token, static_cast<char>(SuccinctGraph::DELIMITERS[0]));

            for (int i = 1; i <= num_actual_delims; ++i) {
                std::getline(
                    iss, token, static_cast<char>(SuccinctGraph::DELIMITERS[i]));
                attr.push_back(token);
            }
        }
        attributes.push_back(attr);
    }
}

// Format: attrIdx1<DELIM>attrKey1<DELIM>attrIdx2<DELIM>attrKey2
// where <DELIM> is GraphFormatter::QUERY_FILED_DELIM.
void generate_node_queries(
    std::string node_file,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file,
    int num_actual_delims,
    bool is_comma_separated = true)
{
    std::vector<std::vector<std::string>> attributes;
    int64_t nodes = 0;
    read_node_attributes(
        attributes, nodes, node_file, num_actual_delims, is_comma_separated);

    size_t num_attributes = attributes.at(0).size();
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, nodes - 1);
    std::uniform_int_distribution<int> uni_attr(0, num_attributes - 1);

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    for (int64_t i = 0; i < warmup_size; i++) {
        int node_id = uni_node(rng);
        int attr1 = uni_attr(rng);
        std::string search_key1 = attributes.at(node_id).at(attr1);
        int attr2 = uni_attr(rng);
        std::string search_key2 = attributes.at(node_id).at(attr2);
        warmup_out << attr1 << GraphFormatter::QUERY_FILED_DELIM
                   << search_key1 << GraphFormatter::QUERY_FILED_DELIM
                   << attr2 << GraphFormatter::QUERY_FILED_DELIM
                   << search_key2 << "\n";
    }

    for (int64_t i = 0; i < query_size; i++) {
        int node_id = uni_node(rng);
        int attr1 = uni_attr(rng);
        std::string search_key1 = attributes.at(node_id).at(attr1);
        int attr2 = uni_attr(rng);
        std::string search_key2 = attributes.at(node_id).at(attr2);
        query_out << attr1 << GraphFormatter::QUERY_FILED_DELIM
                   << search_key1 << GraphFormatter::QUERY_FILED_DELIM
                   << attr2 << GraphFormatter::QUERY_FILED_DELIM
                   << search_key2 << "\n";
    }
}

// Format: randomNodeId,attrIdx,attrKey.
void generate_neighbor_node_queries_no_load(
    std::string node_file,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file,
    int num_actual_delims,
    bool is_comma_separated = true)
{
    std::vector<std::vector<std::string>> attributes;
    int64_t nodes = 0;
    read_node_attributes(
        attributes, nodes, node_file, num_actual_delims, is_comma_separated);

    size_t num_attributes = attributes.at(0).size();
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, nodes - 1);
    std::uniform_int_distribution<int> uni_attr(0, num_attributes - 1);

    auto output = [&](const std::string& out_file, int out_size) {
        std::ofstream out(out_file);
        for (int i = 0; i < out_size; ++i) {
            // randomly select a node ID
            out << uni_node(rng) << ',';
            // randomly select an attr index
            int attr = uni_attr(rng);
            out << attr << ",";
            // randomly select an attr value for that index
            out << attributes.at(uni_node(rng)).at(attr) << std::endl;
        }
    };
    output(warmup_query_file, warmup_size);
    output(query_file, query_size);
}

// Format: randomNodeId,attrIdx,attrKey.
void generate_neighbor_node_queries(
    std::string node_succinct_dir,
    std::string edge_succinct_dir,
    int64_t node_num_attrs,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file)
{
    SuccinctGraph* graph = new SuccinctGraph(
        node_succinct_dir,
        edge_succinct_dir);

    std::random_device rd;
    std::mt19937 rng(rd());

    std::uniform_int_distribution<int64_t> uni_node(0, graph->num_nodes() - 1);
    std::uniform_int_distribution<int> uni_attr(0, node_num_attrs - 1);

    std::ofstream warmup_out(warmup_query_file);
    std::ofstream query_out(query_file);
    std::vector<int64_t> neighbors;
    std::string search_key;

    for (int64_t i = 0; i < warmup_size; i++) {
        int node_id;
        int attr = uni_attr(rng);
        neighbors.clear();
        while (neighbors.empty()) {
            node_id = uni_node(rng);
            graph->get_neighbors(neighbors, node_id);
        }
        std::vector<int64_t>::const_iterator it(neighbors.begin());
        int neighbor_idx = rand() % neighbors.size();
        std::advance(it, neighbor_idx);
        graph->get_attribute(search_key, *it, attr);
        warmup_out << node_id << "," << attr << "," << search_key << "\n";
    }

    for (int64_t i = 0; i < query_size; i++) {
        int node_id;
        int attr = uni_attr(rng);
        neighbors.clear();
        while (neighbors.empty()) {
            node_id = uni_node(rng);
            graph->get_neighbors(neighbors, node_id);
        }
        std::vector<int64_t>::const_iterator it(neighbors.begin());
        int neighbor_idx = rand() % neighbors.size();
        std::advance(it, neighbor_idx);
        graph->get_attribute(search_key, *it, attr);
        query_out << node_id << "," << attr << "," << search_key << "\n";
    }
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
        std::string graph_file =
            node_file.substr(0, node_file.find(".node")) + ".graph";
        bool already_uniquely_delimed = true;
        if (std::strcmp(argv[4], "1")) already_uniquely_delimed = false;

        create_graph_file(
            node_file, edge_file, graph_file, already_uniquely_delimed);

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
        int num_actual_delims = atoi(argv[7]); // not succinct's max bound
        bool is_node_file_comma_separated = true;
        if (std::strcmp(argv[8], "1")) is_node_file_comma_separated = false;
        generate_node_queries(
            node_file,
            warmup_size,
            query_size,
            warmup_file,
            query_file,
            num_actual_delims,
            is_node_file_comma_separated);

    } else if (type == "neighbor-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int warmup_size = atoi(argv[3]);
        int query_size = atoi(argv[4]);
        std::string warmup_file = argv[5];
        std::string query_file = argv[6];
        generate_neighbor_queries(num_nodes,
            warmup_size, query_size,
            warmup_file, query_file);

    } else if (type == "neighbor-node-queries") {

        std::string node_succinct_dir = argv[2];
        std::string edge_succinct_dir = argv[3];
        int64_t node_num_attrs = std::stol(argv[4]);
        int warmup_size = atoi(argv[5]);
        int query_size = atoi(argv[6]);
        std::string warmup_file = argv[7];
        std::string query_file = argv[8];
        generate_neighbor_node_queries(
            node_succinct_dir,
            edge_succinct_dir,
            node_num_attrs,
            warmup_size,
            query_size,
            warmup_file,
            query_file);

    } else if (type == "neighbor-node-queries-noLoad") {

        std::string node_file = argv[2];
        int warmup_size = atoi(argv[3]);
        int query_size = atoi(argv[4]);
        std::string warmup_file = argv[5];
        std::string query_file = argv[6];
        int num_actual_delims = atoi(argv[7]); // not succinct's max bound
        bool is_node_file_comma_separated = true;
        if (std::strcmp(argv[8], "1")) is_node_file_comma_separated = false;
        generate_neighbor_node_queries_no_load(
            node_file,
            warmup_size,
            query_size,
            warmup_file,
            query_file,
            num_actual_delims,
            is_node_file_comma_separated);

    } else if (type == "neighbor-atype-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int max_num_atype = std::atoi(argv[3]);
        int warmup_size = atoi(argv[4]);
        int query_size = atoi(argv[5]);
        std::string warmup_file = argv[6];
        std::string query_file = argv[7];

        generate_neighbor_atype_queries(
            num_nodes,
            max_num_atype,
            warmup_size,
            query_size,
            warmup_file,
            query_file);

    } else if (type == "format-input") {

        std::string edge_list_in(argv[2]);
        std::string attr_file(argv[3]); // TPC-H
        std::string assoc_out_file(argv[4]);
        std::string node_out_file(argv[5]);
        int64_t num_nodes = std::stol(argv[6]);
        int num_node_attr = std::stoi(argv[7]);
        int node_attr_freq = std::stoi(argv[8]);
        int node_attr_size_each = std::stoi(argv[9]);
        char edge_inner_delim = std::string(argv[10]).at(0);
        char edge_end_delim = std::string(argv[11]).at(0);

        std::thread edge_table_thread(
            &GraphFormatter::create_edge_table,
            edge_list_in,
            attr_file,
            assoc_out_file,
            128,
            edge_inner_delim,
            edge_end_delim,
            5,
            -1);

        // 456626 + 1, since unclear if original data is 0-indexed
//        int num_nodes = 456627;
//        int num_attr = 2;
//        int freq = 1000;
//        int len = 350; // so total node attr = len * num_attr

        GraphFormatter::create_node_table(
            node_out_file,
            attr_file,
            num_nodes,
            num_node_attr,
            node_attr_freq,
            node_attr_size_each);

        edge_table_thread.join();

    } else if (type == "graph-construct") {

        std::string node_file(argv[2]);
        std::string edge_file(argv[3]);
        int sa_sr = 64, isa_sr = 64, npa_sr = 256;
        if (argc > 4) {
            sa_sr = std::stoi(argv[4]);
            isa_sr = std::stoi(argv[5]);
            npa_sr = std::stoi(argv[6]);
        }

        SuccinctGraph* graph = new SuccinctGraph("", true); // no-op
        graph->set_npa_sampling_rate(npa_sr);
        graph->set_sa_sampling_rate(sa_sr);
        graph->set_isa_sampling_rate(isa_sr);
        graph->construct(node_file, edge_file);

        printf("SuccinctGraph construction done\n");

    } else if (type == "nodeTable-construct") {

        std::string node_file(argv[2]);
        SuccinctGraph graph("");
        graph.construct_node_table(node_file);

    } else if (type == "neo4j-node") {

        std::string delimed_node_file = argv[2];
        std::string neo4j_node_out = argv[3];

        GraphFormatter::format_neo4j_node_from_node_file(
            delimed_node_file,
            neo4j_node_out
        );

    } else if (type == "neo4j-edge") {

        std::string delimed_edge_file = argv[2];
        std::string neo4j_edge_out = argv[3];

        GraphFormatter::format_neo4j_edge_from_edge_file(
            delimed_edge_file,
            neo4j_edge_out
        );

    } else {
        printf("Unsupported command type: '%s'\n", type.c_str());
        assert(1); // not supported
    }
    return 0;
}
