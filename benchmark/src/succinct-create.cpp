#include <iostream>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <random>
#include <thread>
#include <vector>
#include <list>
#include <limits.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "ports.h"
#include "GraphFormatter.hpp"
#include "SuccinctGraph.hpp"
#include "utils.h"
#include "GraphQueryAggregatorService.h"

using boost::shared_ptr;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

constexpr int DEGREE_LIMIT = 1200; // per atype

void generate_name(std::string& name, int len) {
    for(int i = 0; i < len; i++) {
        name[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

// TODO: code duplicated with init_sharded_benchmark() in GraphBenchmark.
// Assumes the shards and the aggregator processes are started externally, prior
// to the entry into this function.  Processes are assumed to be local for now,
// using sockets ports defined in ports.h.
shared_ptr<GraphQueryAggregatorServiceClient> init_sharded_graph() {
    shared_ptr<GraphQueryAggregatorServiceClient> aggregator(nullptr);
    try {
        LOG_E("Connecting to server...\n");
        shared_ptr<TSocket> socket(
            new TSocket("localhost", QUERY_HANDLER_PORT));
        shared_ptr<TTransport> transport(
                new TBufferedTransport(socket));
        shared_ptr<TProtocol> protocol(
                new TBinaryProtocol(transport));
        aggregator = shared_ptr<GraphQueryAggregatorServiceClient>(
            new GraphQueryAggregatorServiceClient(protocol));
        transport->open();
        LOG_E("Connected to aggregator!\n");

        int ret = aggregator->init();
        LOG_E("Aggregator has init()'d cluster, return code = %d\n", ret);
    } catch (std::exception& e) {
        LOG_E("Exception in initializing sharded graph: %s\n", e.what());
        std::terminate();
    }
    return aggregator;
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
    std::string warmup_file,
    std::string query_file)
{
    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> uni1(0, num_nodes - 1);

    auto output = [&uni1](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        int64_t node;
        for (int64_t i = 0; i < out_size; i++) {
            node = uni1(rng);
            out << node << std::endl;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

void generate_neighbor_queries_limited(
    int64_t num_nodes,
    int warmup_size,
    int query_size,
    std::string warmup_file,
    std::string query_file)
{
    auto aggregator = init_sharded_graph();

    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> uni1(0, num_nodes - 1);

    auto output = [&uni1, aggregator](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        int64_t node;
        for (int64_t i = 0; i < out_size; i++) {
            node = uni1(rng);
            auto cnt = aggregator->assoc_count(node, rand() % 5);
            if (cnt > DEGREE_LIMIT || cnt == 0) {
            	i--;
                continue;
            }
            out << node << std::endl;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

// Format: randomNodeId,atype.
// Since queries are generated randomly, the query results can be empty.
void generate_neighbor_atype_queries(
    int64_t num_nodes,
    int max_num_atype,
    int warmup_size,
    int query_size,
    std::string warmup_file,
    std::string query_file)
{
    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> uni1(0, num_nodes - 1);
    std::uniform_int_distribution<int64_t> uni2(0, max_num_atype - 1);

    auto output = [&uni1, &uni2](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        int64_t node, atype;
        for (int64_t i = 0; i < out_size; i++) {
            node = uni1(rng);
            atype = uni2(rng);
            out << node << "," << atype << std::endl;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

void generate_neighbor_atype_queries_limited(
    int64_t num_nodes,
    int max_num_atype,
    int warmup_size,
    int query_size,
    std::string warmup_file,
    std::string query_file)
{
    auto aggregator = init_sharded_graph();

    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> uni1(0, num_nodes - 1);
    std::uniform_int_distribution<int64_t> uni2(0, max_num_atype - 1);

    auto output = [&uni1, &uni2, aggregator](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        int64_t node, atype;
        for (int64_t i = 0; i < out_size; i++) {
            node = uni1(rng);
            atype = uni2(rng);
            auto cnt = aggregator->assoc_count(node, atype);
            if (cnt > DEGREE_LIMIT || cnt == 0) {
            	i--;
                continue;
            }
            out << node << "," << atype << std::endl;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

void read_node_attributes(
    std::vector<std::vector<std::string>>& attributes,
    int64_t& nodes,
    const std::string& node_attr_file,
    int num_actual_delims,
    bool is_comma_separated)
{
    std::ifstream node_input(node_attr_file);
    std::string line, token;
    std::vector<std::string> attr;
    while (std::getline(node_input, line)) {
        ++nodes;
        attr.clear();
        std::istringstream iss(line);

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
                attr.emplace_back();
                std::getline(
                    iss, attr.back(),
                    static_cast<char>(SuccinctGraph::DELIMITERS[i]));
            }
            assert(attr.size() == num_actual_delims);
        }
        attributes.emplace_back();
        attributes.back() = attr;
    }
}

// Format: attrIdx1<DELIM>attrKey1<DELIM>attrIdx2<DELIM>attrKey2
// where <DELIM> is GraphFormatter::QUERY_FILED_DELIM.
void generate_node_queries(
    size_t num_attributes,
    int warmup_size,
    int query_size,
    std::string warmup_query_file,
    std::string query_file,
    int num_actual_delims,
    bool is_comma_separated = true)
{
    auto aggregator = init_sharded_graph();

    int64_t nodes = 0;

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, nodes - 1);
    std::uniform_int_distribution<int> uni_attr(0, num_attributes - 1);
    std::string search_key1, search_key2;

    auto output = [&](const std::string& out_file, const int64_t out_size) {
        std::ofstream out(out_file);
        for (int64_t i = 0; i < out_size; i++) {
            while (true) {
                int node_id = uni_node(rng);
                int attr1 = uni_attr(rng);
                aggregator->get_attribute(search_key1, node_id, attr1);
                if (search_key1.empty()) {
                    continue;
                }
                int attr2 = uni_attr(rng);
                aggregator->get_attribute(search_key2, node_id, attr2);
                assert(!search_key2.empty());
                out << attr1 << GraphFormatter::QUERY_FILED_DELIM
                   << search_key1 << GraphFormatter::QUERY_FILED_DELIM
                   << attr2 << GraphFormatter::QUERY_FILED_DELIM
                   << search_key2 << "\n";
               break;
           }
        }
        out.close();
    };
    output(warmup_query_file, warmup_size);
    output(query_file, query_size);
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

// Loads the graph (so query results cannot be empty).
// Format: randomNodeId,attrIdx,attrKey.
void generate_neighbor_node_queries(
    int64_t num_nodes,
    int64_t node_num_attrs,
    int warmup_size,
    int query_size,
    std::string warmup_file,
    std::string query_file)
{
    auto aggregator = init_sharded_graph();

    std::random_device rd;
    std::mt19937 rng1(rd()), rng2(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_attr(0, node_num_attrs - 1);

    auto output = [&uni_node, &uni_attr, aggregator](
        const std::string& out_file, int out_size, std::mt19937 my_rng)
    {
        std::ofstream out(out_file);
        std::vector<int64_t> neighbors;
        std::string search_key;
        for (int64_t i = 0; i < out_size; i++) {
            int node_id;
            int attr = uni_attr(my_rng);
            neighbors.clear();
            while (neighbors.empty()) {
                node_id = uni_node(my_rng);
                aggregator->get_neighbors(neighbors, node_id);
            }
            std::vector<int64_t>::const_iterator it(neighbors.begin());
            int neighbor_idx = rand() % neighbors.size();
            std::advance(it, neighbor_idx);
            aggregator->get_attribute(search_key, *it, attr);
            out << node_id << "," << attr << "," << search_key << "\n";
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

void generate_neighbor_node_queries_limited(
    int64_t num_nodes,
    int64_t node_num_attrs,
    int warmup_size,
    int query_size,
    std::string warmup_file,
    std::string query_file)
{
    auto aggregator = init_sharded_graph();

    std::random_device rd;
    std::mt19937 rng1(rd()), rng2(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_attr(0, node_num_attrs - 1);

    auto output = [&uni_node, &uni_attr, aggregator](
        const std::string& out_file, int out_size, std::mt19937 my_rng)
    {
        std::ofstream out(out_file);
        std::vector<int64_t> neighbors;
        std::string search_key;
        for (int64_t i = 0; i < out_size; i++) {
            int node_id;
            int attr = uni_attr(my_rng);
            neighbors.clear();
            while (neighbors.empty()) {
                node_id = uni_node(my_rng);
                aggregator->get_neighbors(neighbors, node_id);
                if (neighbors.size() > DEGREE_LIMIT * 5) {
                    neighbors.clear();
                    continue;
                }
            }
            std::vector<int64_t>::const_iterator it(neighbors.begin());
            int neighbor_idx = rand() % neighbors.size();
            std::advance(it, neighbor_idx);
            aggregator->get_attribute(search_key, *it, attr);
            out << node_id << "," << attr << "," << search_key << "\n";
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

// Samples (nodeId, atype) uniformly at random.  Fix off=0, limit=1000.
void generate_tao_assoc_range_queries(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    const std::string& warmup_file, const std::string& query_file)
{
    std::random_device rd;
    std::mt19937 rng1(rd()), rng2(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_atype(0, max_num_atype - 1);

    auto output = [&uni_node, &uni_atype](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        int i = 0;
        // constants
        int off = 0;
        int len = 1000;
        while (i < out_size) {
            int64_t node_id = uni_node(rng);
            int atype = uni_atype(rng);
            out << node_id << "," << atype << ",";
            out << off << "," << len << std::endl;
            ++i;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

void generate_tao_assoc_range_queries_limited(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    const std::string& warmup_file, const std::string& query_file)
{
    auto aggregator = init_sharded_graph();
    std::random_device rd;
    std::mt19937 rng1(rd()), rng2(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_atype(0, max_num_atype - 1);

    auto output = [&uni_node, &uni_atype, aggregator](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        int i = 0;
        // constants
        int off = 0;
        int len = 1000;
        while (i < out_size) {
            int64_t node_id = uni_node(rng);
            int atype = uni_atype(rng);

            auto cnt = aggregator->assoc_count(node_id, atype);
            if (cnt > DEGREE_LIMIT || cnt == 0) {
                continue;
            }

            out << node_id << "," << atype << ",";
            out << off << "," << len << std::endl;
            ++i;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

// Helper function that scans the assoc file and look for global minimum and
// maximum timestamps.
std::pair<int64_t, int64_t> get_min_max_timestamp(const std::string& assoc_file)
{
    std::ifstream ifs(assoc_file);
    std::string line, token;
    int64_t min_time = 1LL << 60, max_time = -1, time;
    while (std::getline(ifs, line)) {
        std::stringstream ss(line);
        std::getline(ss, token, ' '); // src_id
        std::getline(ss, token, ' '); // dst_id
        std::getline(ss, token, ' '); // atype
        std::getline(ss, token, ' '); // bingo: timestamp
        time = std::stoll(token);
        min_time = std::min(min_time, time);
        max_time = std::max(max_time, time);
    }
    return std::make_pair(min_time, max_time);
}

// Shared between assoc_get() and assoc_time_range().
void generate_tao_time_related_queries_helper(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    //const std::string& assoc_file,
    const std::string& warmup_file,
    const std::string& query_file,
    bool for_assoc_get)
{
    // auto pair = get_min_max_timestamp(assoc_file);
    // int64_t min_time = pair.first, max_time = pair.second;
    // LOG_E("Assoc scan finishes: min timestamp %lld, max timestamp %lld\n",
    //    min_time, max_time);

    auto aggregator = init_sharded_graph();

    std::random_device rd;
    std::mt19937 rng1(rd()), rng2(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_atype(0, max_num_atype - 1);
    // std::uniform_int_distribution<int64_t> uni_time(min_time, max_time);
    std::uniform_int_distribution<int> uni_put(0, 1);

    auto output = [&](
        const std::string& out_file, int out_size, std::mt19937 rng)
    {
        std::ofstream out(out_file);
        std::vector<int64_t> vec;
        int i = 0;
        while (i < out_size) {
            int64_t node_id = uni_node(rng);
            int atype = uni_atype(rng);

            if (for_assoc_get) {
                aggregator->get_neighbors_atype(vec, node_id, atype);
                if (vec.empty()) {
                    continue;
                }
            }

            // int64_t t1 = uni_time(rng);
            // int64_t t2 = uni_time(rng);
            out << node_id << "," << atype << ","
                << 0 << "," << std::numeric_limits<int64_t>::max();

            if (for_assoc_get) {
                for (int64_t dst_id : vec) {
                    if (uni_put(rng) == 0) { // put w/ probability 1/2
                        out << "," << dst_id;
                    }
                }
            } else {
                // assoc_time_range(): fix limit = 1000, same as assoc_range.
                int limit = 1000;
                out << "," << limit;
            }
            out << std::endl;
            ++i;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

void generate_tao_time_related_queries_helper_limited(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    // const std::string& assoc_file,
    const std::string& warmup_file,
    const std::string& query_file,
    bool for_assoc_get)
{
    // auto pair = get_min_max_timestamp(assoc_file);
    // int64_t min_time = pair.first, max_time = pair.second;
    // LOG_E("Assoc scan finishes: min timestamp %lld, max timestamp %lld\n",
    //    min_time, max_time);

    auto aggregator = init_sharded_graph();

    std::random_device rd;
    std::mt19937 rng1(rd()), rng2(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_atype(0, max_num_atype - 1);
    // std::uniform_int_distribution<int64_t> uni_time(min_time, max_time);
    std::uniform_int_distribution<int> uni_put(0, 1);

    auto output = [&](
        const std::string& out_file, int out_size, std::mt19937 my_rng)
    {
        std::ofstream out(out_file);
        std::vector<int64_t> vec;
        int i = 0;
        while (i < out_size) {
            int64_t node_id = uni_node(my_rng);
            int atype = uni_atype(my_rng);

            auto cnt = aggregator->assoc_count(node_id, atype);
            if (cnt > DEGREE_LIMIT || cnt == 0) {
                continue;
            }

            if (for_assoc_get) {
                aggregator->get_neighbors_atype(vec, node_id, atype);
                if (vec.empty()) {
                    continue;
                }
            }

            // int64_t t1 = uni_time(my_rng);
            // int64_t t2 = uni_time(my_rng);
            out << node_id << "," << atype << ","
                << 0 << "," << std::numeric_limits<int64_t>::max();

            if (for_assoc_get) {
                for (int64_t dst_id : vec) {
                    if (uni_put(my_rng) == 0) { // put w/ probability 1/2
                        out << "," << dst_id;
                    }
                }
            } else {
                // assoc_time_range(): fix limit = 1000, same as assoc_range.
                int limit = 1000;
                out << "," << limit;
            }
            out << std::endl;
            ++i;
        }
    };
    output(warmup_file, warmup_size, rng1);
    output(query_file, query_size, rng2);
}

// Scans the assoc file and for global minimum and maximum timestamps.  Then,
// similar to assoc_range()'s query gen, with the change that `low` and `high`
// are uniformly sampled in [globalMinTime, globalMaxTime].
//
// For `dst_id_set`, we load the graph and put each of the actual dst ids in
// an assoc list into the set, independently with probability 1/2.  The purpose
// is to not make this query behave the same as assoc_time_range w/ limit infty.
//
// Query output format, each line: src,atype,low,high,[dstId,]+
void generate_tao_assoc_get_queries(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    // const std::string& assoc_file,
    const std::string& warmup_file, const std::string& query_file)
{
    generate_tao_time_related_queries_helper(num_nodes, max_num_atype,
        warmup_size, query_size, //assoc_file,
		warmup_file, query_file, true);
}

void generate_tao_assoc_get_queries_limited(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    // const std::string& assoc_file,
    const std::string& warmup_file, const std::string& query_file)
{
    generate_tao_time_related_queries_helper_limited(num_nodes, max_num_atype,
        warmup_size, query_size, // assoc_file,
		warmup_file, query_file, true);
}

// Generates time ranges similar to assoc_get().  Sets `limit` to 1000.
//
// Format, each line: src,atype,low,high,limit
void generate_tao_assoc_time_range_queries(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    // const std::string& assoc_file,
    const std::string& warmup_file, const std::string& query_file)
{
    generate_tao_time_related_queries_helper(num_nodes, max_num_atype,
        warmup_size, query_size, // assoc_file,
		warmup_file, query_file, false);
}

void generate_tao_assoc_time_range_queries_limited(
    int64_t num_nodes, int max_num_atype,
    int warmup_size, int query_size,
    // const std::string& assoc_file,
    const std::string& warmup_file, const std::string& query_file)
{
    generate_tao_time_related_queries_helper_limited(num_nodes, max_num_atype,
        warmup_size, query_size, // assoc_file,
		warmup_file, query_file, false);
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

        int64_t num_attributes = std::stoll(argv[2]);
        int warmup_size = atoi(argv[3]);
        int query_size = atoi(argv[4]);
        std::string warmup_file = argv[5];
        std::string query_file = argv[6];
        int num_actual_delims = atoi(argv[7]); // not succinct's max bound
        bool is_node_file_comma_separated = true;
        if (std::strcmp(argv[8], "1")) is_node_file_comma_separated = false;
        generate_node_queries(
            num_attributes,
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
        // non empty
        generate_neighbor_queries_limited(num_nodes,
            warmup_size, query_size,
            warmup_file, query_file);

    } else if (type == "neighbor-node-queries") {

        std::string node_succinct_dir = argv[2];
        std::string edge_succinct_dir = argv[3];
        int64_t num_nodes = std::stoll(argv[4]);
        int64_t node_num_attrs = std::stol(argv[5]);
        int warmup_size = atoi(argv[6]);
        int query_size = atoi(argv[7]);
        std::string warmup_file = argv[8];
        std::string query_file = argv[9];
        // non empty
        generate_neighbor_node_queries_limited(
            num_nodes,
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

        // non empty
        generate_neighbor_atype_queries_limited(
            num_nodes,
            max_num_atype,
            warmup_size,
            query_size,
            warmup_file,
            query_file);

    } else if (type == "tao-node-get-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int warmup_size = std::stoi(argv[3]);
        int query_size = std::stoi(argv[4]);
        std::string warmup_file = argv[5];
        std::string query_file = argv[6];

        generate_neighbor_queries(num_nodes,
            warmup_size, query_size, warmup_file, query_file);

    } else if (type == "tao-assoc-range-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int max_num_atype = std::stoi(argv[3]);
        int warmup_size = std::stoi(argv[4]);
        int query_size = std::stoi(argv[5]);
        std::string warmup_file = argv[6];
        std::string query_file = argv[7];

        // non empty
        generate_tao_assoc_range_queries_limited(
            num_nodes, max_num_atype, warmup_size, query_size,
            warmup_file, query_file);

    } else if (type == "tao-assoc-get-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int max_num_atype = std::stoi(argv[3]);
        int warmup_size = std::stoi(argv[4]);
        int query_size = std::stoi(argv[5]);
        // std::string assoc_file(argv[6]);
        std::string warmup_file(argv[6]);
        std::string query_file(argv[7]);

        // non empty
        generate_tao_assoc_get_queries_limited(
            num_nodes, max_num_atype, warmup_size, query_size, // assoc_file,
			warmup_file, query_file);

    } else if (type == "tao-assoc-count-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int max_num_atype = std::stoi(argv[3]);
        int warmup_size = std::stoi(argv[4]);
        int query_size = std::stoi(argv[5]);
        std::string warmup_file(argv[6]);
        std::string query_file(argv[7]);

        // non empty
        generate_neighbor_atype_queries_limited(
            num_nodes, max_num_atype, warmup_size, query_size,
            warmup_file, query_file);

    } else if (type == "tao-assoc-time-range-queries") {

        int64_t num_nodes = std::stoll(argv[2]);
        int max_num_atype = std::stoi(argv[3]);
        int warmup_size = std::stoi(argv[4]);
        int query_size = std::stoi(argv[5]);
        // std::string assoc_file(argv[6]);
        std::string warmup_file(argv[6]);
        std::string query_file(argv[7]);

        // non empty
        generate_tao_assoc_time_range_queries_limited(
            num_nodes, max_num_atype, warmup_size, query_size, // assoc_file,
			warmup_file, query_file);

    } else if (type == "format-input") {

        std::string edge_list_in(argv[2]);
        std::string attr_file(argv[3]); // TPC-H
        std::string assoc_out_file(argv[4]);
        std::string node_out_file(argv[5]);
        int64_t num_nodes = std::stol(argv[6]);
        int num_node_attr = std::stoi(argv[7]);
        int zipf_corpus_size = std::stoi(argv[8]);
        int node_attr_size_each = std::stoi(argv[9]);
        char edge_inner_delim = std::string(argv[10]).at(0);
        char edge_end_delim = std::string(argv[11]).at(0);

        int min_out_degree = -1;
        if (argc >= 13) {
            min_out_degree = std::stoi(argv[12]);
        }
        bool assign_ts_attr = false;
        if (argc >= 14) {
            assign_ts_attr = true;
        }

        std::thread edge_table_thread(
            &GraphFormatter::create_edge_table,
            edge_list_in,
            attr_file,
            assoc_out_file,
            128,
            edge_inner_delim,
            edge_end_delim,
            5,
            min_out_degree,
            assign_ts_attr);

//        GraphFormatter::create_node_table_zipf(
//            node_out_file,
//            attr_file,
//            num_nodes,
//            num_node_attr,
//            node_attr_size_each,
//            zipf_corpus_size);

        edge_table_thread.join();
        LOG_E("thread for file '%s' done\n", edge_list_in.c_str());

    } else if (type == "create-nodeTable") {

        std::string attr_file(argv[2]); // TPC-H
        std::string node_out_file(argv[3]);
        int64_t num_nodes = std::stol(argv[4]);
        int num_node_attr = std::stoi(argv[5]);
        int zipf_corpus_size = std::stoi(argv[6]);
        int node_attr_size_each = std::stoi(argv[7]);

        GraphFormatter::create_node_table_zipf(
            node_out_file,
            attr_file,
            num_nodes,
            num_node_attr,
            node_attr_size_each,
            zipf_corpus_size);

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

    } else if (type == "make-store") {

        std::string store_out = argv[2];
        size_t num_edges_to_add = std::stoll(argv[3]);
        size_t num_nodes = std::stoll(argv[4]);
        size_t num_atypes = atoi(argv[5]);
        std::string assoc_set_file = argv[6];
        std::string attr_file = argv[7];
        int bytes_per_attr = atoi(argv[8]);
        int64_t min_time = std::stoll(argv[9]);
        int64_t max_time = std::stoll(argv[10]);
        int suffix_or_log = atoi(argv[11]); // 0 for Suffix, 1 for log

        if (suffix_or_log == 0) {
            GraphFormatter::make_rand_suffix_store(
                store_out,
                num_edges_to_add,
                num_nodes,
                num_atypes,
                assoc_set_file,
                attr_file,
                bytes_per_attr,
                min_time,
                max_time);
        } else {
            GraphFormatter::make_rand_log_store(
                store_out,
                num_edges_to_add,
                num_nodes,
                num_atypes,
                assoc_set_file,
                attr_file,
                bytes_per_attr,
                min_time,
                max_time);
        }

    } else {
        printf("Unsupported command type: '%s'\n", type.c_str());
        assert(1); // not supported
    }
    return 0;
}
