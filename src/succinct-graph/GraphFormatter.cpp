#include "succinct-graph/GraphFormatter.hpp"

#include <cassert>
#include <fstream>
#include <sstream>
#include <vector>
#include <random>
#include <sys/time.h>

#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/utils.h"

int64_t time_millis() {
    struct timeval now;
    gettimeofday(&now, NULL);
    return (int64_t) now.tv_sec * 1000 + now.tv_usec / 1000;
}

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


void GraphFormatter::create_node_table(
    const std::string& out_file,
    const std::string& attr_file,
    int num_nodes,
    int num_attr,
    int freq,
    int len) {

    assert(num_attr <= SuccinctGraph::MAX_NUM_NODE_ATTRS);

    std::ifstream attr_in_stream(attr_file);
    std::ofstream s_out(out_file);
    std::vector<std::vector<std::string>> attributes;
    std::string attr, tmp;

    for (int j = 0; j < num_attr; ++j) {
        // need attributes for column `attr`, length num_nodes
        int n = num_nodes;
        std::vector<std::string> attrs;
        attr.clear();
        while (n > 0) {
            // multiple records concatenated together
            while (attr.length() < len) {
                std::getline(attr_in_stream, tmp);
                attr += tmp;
            }
            tmp = attr.substr(0, len); // possibly truncate
            attr.erase(0, len); // can potentially reuse this row

            int i = 0;
            while (n > 0 && i < freq) {
                attrs.push_back(tmp);
                --n;
                ++i;
            }
        }
        std::random_shuffle(attrs.begin(), attrs.end());
        attributes.push_back(attrs);
    }
    attr_in_stream.close();
    assert(attributes.size() == num_attr &&
        attributes.at(0).size() == num_nodes);

    std::vector<std::string> node_attrs;
    for (int i = 0; i < num_nodes; i++) {
        node_attrs.clear();
        for (int attr = 0; attr < num_attr; attr++) {
            assert(attributes[attr][i].find(static_cast<char>(
                SuccinctGraph::DELIMITERS[attr])) == std::string::npos); // weak
            node_attrs.push_back(attributes[attr][i]);
        }
        s_out << GraphFormatter::format_node_attrs_str({ node_attrs });
    }
    s_out.close();
}

void GraphFormatter::create_edge_table(
    const std::string& file,
    const std::string& attr_file,
    const std::string& out_file,
    int bytes_per_attr,
    int num_atype,
    bool has_atype_timestamp)
{
    std::ifstream in_stream(file);
    std::ifstream attr_in_stream(attr_file);
    std::string line, token;

    std::map<SuccinctGraph::AssocListKey, std::vector<SuccinctGraph::Assoc>>
        assoc_map;
    SuccinctGraph::AType atype;
    SuccinctGraph::Timestamp time;
    SuccinctGraph::NodeId src_id, dst_id;

    std::random_device rd1, rd2;
    std::mt19937 rng1(rd1()), rng2(rd2());
    std::uniform_int_distribution<int64_t> atype_dis(0, num_atype);
    std::uniform_int_distribution<int64_t> time_dis(
        0, std::numeric_limits<int>::max());

    while (std::getline(in_stream, line)) {
        if (line[0] == '#') {
            continue;
        }

        std::stringstream ss(line);
        int token_idx = 0;
        while (std::getline(ss, token, ' ')) {
            ++token_idx;
            if (token_idx == 1) src_id = std::stol(token);
            else if (token_idx == 2) dst_id = std::stol(token);
            else if (token_idx == 3) time = std::stol(token);
            else if (token_idx == 4) {
                if (token == "MT") atype = 0;
                else if (token == "RE") atype = 1;
                else if (token == "RT") atype = 2;
                else assert(0);
            }
            token.clear();
            if (!has_atype_timestamp && token_idx == 2) {
                // Generate atype and timestamp
                atype = atype_dis(rng1);
                // C.f. LinkBench
                // Choose something from now back to about 50 days
                // return (System.currentTimeMillis() - Integer.MAX_VALUE - 1L)
                //                                        + rng.nextInt();
                time = time_millis() - std::numeric_limits<int>::max()
                    - 1 + time_dis(rng2);
                break;
            }
            if (has_atype_timestamp && token_idx == 4) break;
        }

        if (attr_in_stream.eof()) {
            // if attrs exhausted, recycle
            attr_in_stream.close();
            attr_in_stream.open(attr_file);
        }
        std::string attr;
        std::getline(attr_in_stream, attr);
        if (attr.length() > bytes_per_attr) {
            attr = attr.substr(0, bytes_per_attr);
        } else {
            // just pad with '|'
            attr += std::string(bytes_per_attr - attr.length(), '|');
        }
        SuccinctGraph::Assoc assoc = { src_id, dst_id, atype, time, attr };
        assoc_map[std::make_pair(src_id, atype)].push_back(assoc);
    }
    in_stream.close();
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
    std::vector<std::vector<std::string>> node_attrs) {

    std::string result, formatted;
    for (auto attrs : node_attrs) {
        assert(attrs.size() <= SuccinctGraph::MAX_NUM_NODE_ATTRS);
        formatted.clear();
        for (int i = 0; i < attrs.size(); ++i) {
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
            int pos = line.find(static_cast<char>(node_attr_delim));
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
