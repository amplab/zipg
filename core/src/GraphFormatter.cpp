#include "GraphFormatter.hpp"

#include <cassert>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <sys/time.h>

#include "SuccinctGraph.hpp"
#include "ZipfGenerator.hpp"
#include "utils.h"

// Private helper func
std::string gen_random_string(const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string res(len, ' ');
    for (int i = 0; i < len; ++i) {
        res[i] = alphanum[std::rand() % (sizeof(alphanum) - 1)];
    }
    return res;
}

// Private helper func
std::vector<std::string> gen_random_strings(int num_str, int freq, int len) {
    std::string name = std::string(len, '0');
    std::vector<std::string> attrs;
    while (num_str > 0) {
        std::string attr = gen_random_string(len);
        int i = 0;
        while (num_str > 0 && i < freq) {
            attrs.push_back(attr);
            num_str--;
            i++;
        }
    }
    std::random_shuffle(attrs.begin(), attrs.end());
    return attrs;
}

void GraphFormatter::format_node_file(const std::string& node_file) {
    std::ifstream node_input(node_file);
    // each string == all attributes for a node
    std::vector<std::string> node_attrs;
    std::string line;

    for (int nodes = 0; !node_input.eof(); nodes++) {
        std::getline(node_input, line, '\n');
        if (line.length() == 0)
            break;
        line = ',' + line; // prepend each data element with a comma
        int pos = -1;
        // replace commas, e.g. ",attr1,attr2,attr3" -> "∆attr1$attr2*att3"
        for (unsigned char delim: SuccinctGraph::DELIMITERS) {
            pos = line.find(',', pos + 1);
            line[pos] = static_cast<char>(delim);
        }
        assert(line.find(',', pos + 1) == std::string::npos); // no more attr
        node_attrs.push_back(line);
    }
    node_input.close();

    std::string node_out_file = node_file + "_formatted";
    std::ofstream s_out(node_out_file);
    for (auto it = node_attrs.begin(); it != node_attrs.end(); ++it) {
        s_out << *it << "\n";
    }
    s_out.close();
}

void GraphFormatter::create_random_node_table(
    const std::string& out_file,
    int num_nodes,
    int num_attr,
    int freq,
    int len) {

    assert(num_attr <= SuccinctGraph::MAX_NUM_NODE_ATTRS);

    std::ofstream s_out(out_file);
    std::vector<std::vector<std::string>> attributes;
    for (int attr = 0; attr < num_attr; attr++) {
        attributes.push_back(gen_random_strings(num_nodes, freq, len));
    }
    for (int i = 0; i < num_nodes; i++) {
        for (int attr = 0; attr < num_attr; attr++) {
            s_out << SuccinctGraph::DELIMITERS[attr]
                  << attributes[attr][i];
        }
        s_out << "\n";
    }
    s_out.close();
}

void GraphFormatter::create_node_table_zipf(
    const std::string& out_file,
    const std::string& attr_file,
    int num_nodes,
    int num_attr,
    int len,
    int corpus_size,
    bool report_freq_dist)
{
    assert(num_attr <= SuccinctGraph::MAX_NUM_NODE_ATTRS);

    std::ifstream attr_in_stream(attr_file);
    std::string attr, tmp;
    size_t length = static_cast<size_t>(len);

    ZipfGenerator zipf(0.99, corpus_size);
    std::unordered_map<size_t, int> idx_to_freq; // for fast inserts
    size_t item_idx;

    std::vector<std::string> corpus_vec, selected_attrs;
    std::set<std::string> corpus;

    // Populate the corpus
    while (corpus.size() < corpus_size) {
        if (corpus.size() % 100000 == 0)  {
            LOG_E("Corpus size: %d\n", corpus.size());
        }
        // multiple records concatenated together
        while (attr.length() < length) {
            if (attr_in_stream.eof()) {
               // if attrs exhausted, recycle
               attr_in_stream.close();
               attr_in_stream.open(attr_file);
            }
            std::getline(attr_in_stream, tmp);
            attr += tmp;
        }
        corpus.insert(attr.substr(0, length)); // possibly truncate
        attr.erase(0, length); // can potentially reuse this row
    }
    attr_in_stream.close();
    std::copy(corpus.begin(), corpus.end(), std::back_inserter(corpus_vec));

    std::ofstream node_table_out(out_file);
    std::vector<std::string> node_attrs;
    node_attrs.resize(num_attr);

    for (size_t node = 0; node < num_nodes; ++node) {
        for (int i = 0; i < num_attr; ++i) {
            node_attrs[i] = corpus_vec[zipf.next()];
        }
        node_table_out << GraphFormatter::format_node_attrs_str({ node_attrs });
    }

    if (report_freq_dist) {
        LOG_E("Sorry, refactored so reports are not available!\n");
    }
}

void GraphFormatter::create_node_table(
    const std::string& out_file,
    const std::string& attr_file,
    int num_nodes,
    int num_attr,
    int freq,
    int len)
{
    assert(num_attr <= SuccinctGraph::MAX_NUM_NODE_ATTRS);

    std::ifstream attr_in_stream(attr_file);
    std::ofstream s_out(out_file);
    std::vector<std::vector<std::string>> attributes;
    std::string attr, tmp;
    size_t length = static_cast<size_t>(len);
    std::map<std::string, int> attr_to_freq;

    for (int j = 0; j < num_attr; ++j) {
        // need attributes for column `attr`, length num_nodes
        int n = num_nodes;
        std::vector<std::string> attrs;
        attr.clear();
        while (n > 0) {
            // multiple records concatenated together
            while (attr.length() < length) {
                std::getline(attr_in_stream, tmp);
                attr += tmp;
            }
            tmp = attr.substr(0, length); // possibly truncate
            attr.erase(0, length); // can potentially reuse this row

            while (n > 0 && attr_to_freq[tmp] < freq) {
                attrs.push_back(tmp);
                --n;
                ++attr_to_freq[tmp];
            }
        }
        std::random_shuffle(attrs.begin(), attrs.end());
        attributes.push_back(attrs);
    }
    assert(attributes.size() == static_cast<size_t>(num_attr) &&
        attributes.at(0).size() == static_cast<size_t>(num_nodes));

    output_node_attributes(out_file, attributes, num_nodes, num_attr);
}

std::vector<std::vector<int64_t>> GraphFormatter::read_edge_list(
    const std::string& file,
    char edge_inner_delim,
    char edge_end_delim)
{
    std::ifstream in_stream(file);
    std::string line, token;
    std::vector<std::vector<int64_t>> result;
    while (std::getline(in_stream, line)) {
        if (line[0] == '#') {
            continue;
        }
        std::stringstream ss(line);
        std::getline(ss, token, edge_inner_delim);
        int64_t src_id = std::stoll(token);
        std::getline(ss, token, edge_end_delim);
        int64_t dst_id = std::stoll(token);
        result[src_id].push_back(dst_id);
    }
    return result;
}

void GraphFormatter::make_rand_assoc(
    SuccinctGraph::Assoc& assoc,
    int64_t src_id,
    int64_t dst_id,
    const std::string& attr_file,
    std::ifstream& attr_in_stream,
    int bytes_per_attr,
    std::uniform_int_distribution<int64_t> atype_dis,
    std::mt19937& atype_rng,
    std::uniform_int_distribution<int> time_dis,
    std::mt19937& time_rng,
    bool augmented_assoc,
    bool assign_ts_attr_to_dummy)
{
    // Generate atype and timestamp
    SuccinctGraph::AType atype = atype_dis(atype_rng);
    // C.f. LinkBench
    // Choose something from now back to about 50 days
    // return (System.currentTimeMillis() - Integer.MAX_VALUE - 1L)
    //                                        + rng.nextInt();
    SuccinctGraph::Timestamp time = time_millis() -
       std::numeric_limits<int>::max() - 1 + time_dis(time_rng);

    if (attr_in_stream.eof()) {
       // if attrs exhausted, recycle
       attr_in_stream.close();
       attr_in_stream.open(attr_file);
    }
    std::string attr;
    // introduce some randomness so that threads don't gen. the same attributes
    std::getline(attr_in_stream, attr);
    if (rand() % 2 == 0) {
        std::getline(attr_in_stream, attr);
    }
    if (attr.length() > static_cast<size_t>(bytes_per_attr)) {
       attr = attr.substr(0, bytes_per_attr);
    } else {
       // just pad with '|'
       attr += std::string(bytes_per_attr - attr.length(), '|');
    }
    if (!augmented_assoc || assign_ts_attr_to_dummy) {
       // There are two cases where we assign a random timestamp and an
       // extracted attribute:
       // (1) Either the edge is a real, existing edge, or
       // (2) the edge is an augmented dummy edge, and
       // `assign_ts_attr_to_dummy` is set to true.
       assoc = { src_id, dst_id, atype, time, attr };
    } else {
       // UNSAFE: Augmented edges are assigned empty attribute value,
       // which is unsafe since the existing assoc lists likely already
       // have a positive edge attr width.
       assoc = { src_id, dst_id, atype, 0, "" };
    }
}

void GraphFormatter::create_edge_table(
    const std::string& file,
    const std::string& attr_file,
    const std::string& out_file,
    int bytes_per_attr,
    char edge_inner_delim,
    char edge_end_delim,
    int num_atype,
    int min_out_degree,
    bool assign_ts_attr_to_dummy_edges)
{
    std::ifstream in_stream(file);
    std::ifstream attr_in_stream(attr_file);
    std::string line, token;

    std::map<SuccinctGraph::AssocListKey, std::vector<SuccinctGraph::Assoc>>
        assoc_map;
    SuccinctGraph::NodeId src_id, dst_id;

    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> atype_dis(0, num_atype - 1);
    std::uniform_int_distribution<int> time_dis(
        0, std::numeric_limits<int>::max());

    std::vector<std::set<int64_t>> edge_list_set;
    int64_t max_node = -1;

    while (std::getline(in_stream, line)) {
        if (line[0] == '#') {
            continue;
        }
        std::stringstream ss(line);
        std::getline(ss, token, edge_inner_delim);
        src_id = std::stoll(token);
        std::getline(ss, token, edge_end_delim);
        dst_id = std::stoll(token);

        SuccinctGraph::Assoc assoc;
        GraphFormatter::make_rand_assoc(
            assoc, src_id, dst_id,
            attr_file, attr_in_stream, bytes_per_attr,
            atype_dis, rng1, time_dis, rng2);
        assoc_map[std::make_pair(src_id, assoc.atype)].push_back(assoc);

        // maintain info for augmentation
        if (src_id >= edge_list_set.size()) {
          edge_list_set.resize(src_id + 1);
        }
        edge_list_set[src_id].insert(dst_id);
        max_node = std::max(max_node, src_id);
        max_node = std::max(max_node, dst_id);
    }
    in_stream.close();

    // augment!
    if (min_out_degree != -1) {
        assert(min_out_degree > 0);
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int64_t> dist(0, max_node);
        int64_t total_edges = 0;

        for (int64_t node_id = 0; node_id <= max_node; ++node_id) {
            // TODO: we need a few asserts here to be safe...
            while (edge_list_set[node_id].size() < min_out_degree) {
                int64_t dst_id = dist(rng);
                if (edge_list_set[node_id].count(dst_id) == 0) {
                    edge_list_set[node_id].insert(dst_id);

                    // also insert into assoc map
                    SuccinctGraph::Assoc assoc;
                    GraphFormatter::make_rand_assoc(
                        assoc, node_id, dst_id,
                        attr_file, attr_in_stream, bytes_per_attr,
                        atype_dis, rng1, time_dis, rng2,
                        true, assign_ts_attr_to_dummy_edges);
                    assoc_map[std::make_pair(node_id, assoc.atype)].push_back(
                        assoc);
                }
            }
            total_edges += edge_list_set[node_id].size();
        }
        LOG_E("Augmentation done, #nodes %" PRId64 ", #edges %" PRId64 ", "
            "avg deg %.1f\n",
            max_node + 1, total_edges, total_edges * 1. / (max_node + 1));
    }
    attr_in_stream.close();

    std::ofstream out_stream(out_file);
    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        auto src_id_and_atype = it->first;
        auto assoc_list = it->second;
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            auto assoc = *it2;
            out_stream << src_id_and_atype.first << " "
                       << assoc.dst_id << " "
                       << src_id_and_atype.second << " "
                       << assoc.time << " "
                       << assoc.attr << "\n";
        }
    }
    out_stream.close();

    printf("Formatted edge file saved to %s\n", out_file.c_str());
}

std::string GraphFormatter::format_node_attrs_str(
    const std::vector<std::vector<std::string>>& node_attrs)
{
    std::string result, formatted;
    for (auto& attrs : node_attrs) {
        assert(attrs.size() <= SuccinctGraph::MAX_NUM_NODE_ATTRS);
        formatted.clear();
        for (size_t i = 0; i < attrs.size(); ++i) {
            assert(attrs[i].find(static_cast<char>(
                SuccinctGraph::DELIMITERS[i])) == std::string::npos); // weak check
            formatted += static_cast<char>(SuccinctGraph::DELIMITERS[i]) + attrs[i];
        }
        // append delims for empty attr slots, AND end-of-record delim
        for (int j = attrs.size(); j <= SuccinctGraph::MAX_NUM_NODE_ATTRS; ++j)
        {
            formatted += static_cast<char>(SuccinctGraph::DELIMITERS[j]);
        }
        result += formatted + '\n';
    }
    return result;
}

void GraphFormatter::format_neo4j_node_from_node_file(
    const std::string& delimed_node_file,
    const std::string& neo4j_node_out,
    char neo4j_delim) {

    std::ifstream in_stream(delimed_node_file);
    std::ofstream out_stream(neo4j_node_out);
    std::string line;

    int node_id = 0;
    while (std::getline(in_stream, line)) {
        out_stream << node_id;
        // parse the delim-ed node attrs, replace all delims with neo4j_delim
        for (unsigned char node_attr_delim : SuccinctGraph::DELIMITERS) {
            size_t pos = line.find(static_cast<char>(node_attr_delim));
            // NOTE: hacky! This assumes input uses the first few delims
            // consecutively only.
            if (pos == std::string::npos) break;
            line[pos] = neo4j_delim;
        }
        out_stream << line << "\n";
        ++node_id;
    }

    in_stream.close();
    out_stream.close();
}

void GraphFormatter::format_neo4j_edge_from_edge_file(
    const std::string& delimed_edge_file,
    const std::string& neo4j_edge_out,
    char neo4j_delim) {

    std::ifstream in_stream(delimed_edge_file);
    std::ofstream out_stream(neo4j_edge_out);
    std::string line, token;

    while (std::getline(in_stream, line)) {
        std::istringstream iss(line);

        std::getline(iss, token, ' '); // src id
        out_stream << token << neo4j_delim;

        std::getline(iss, token, ' '); // dst id
        out_stream << token << neo4j_delim;

        std::getline(iss, token, ' '); // atype
        out_stream << token << neo4j_delim;

        std::getline(iss, token, ' '); // timestamp
        out_stream << token << neo4j_delim;

        std::getline(iss, token, '\n'); // rest is attr
        out_stream << token << "\n";
    }

    in_stream.close();
    out_stream.close();
}


int64_t GraphFormatter::format_lengths_of_attrs(
    const std::string& delimed, std::vector<int64_t>& attr_lengths)
{
    std::string token;
    std::stringstream ss(delimed);
    attr_lengths.clear();
    int distance = 0;

    // skip first delim
    std::getline(ss, token, static_cast<char>(SuccinctGraph::DELIMITERS[0]));

    // Need to reach DELIMITERS[MAX] as well
    for (int i = 1; i <= SuccinctGraph::MAX_NUM_NODE_ATTRS; ++i) {
        // assumes consecutive use of the delimiters
        if (!std::getline(
            ss, token, static_cast<char>(SuccinctGraph::DELIMITERS[i])))
        {
            break;
        }
        attr_lengths.push_back(token.length());
        // account for one delimiter after each len here
        distance += num_digits(token.length()) + 1;
    }
    return distance;
}
