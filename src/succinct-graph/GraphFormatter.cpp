#include "succinct-graph/GraphFormatter.hpp"
#include "succinct-graph/SuccinctGraph.hpp"

#include <cassert>
#include <fstream>
#include <sstream>
#include <vector>

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
        for (char delim: SuccinctGraph::DELIMINATORS) {
            pos = line.find(',', pos + 1);
            line[pos] = delim;
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

void GraphFormatter::format_higgs_activity_file(
    const std::string& file,
    const std::string& out_file,
    int bytes_per_attr) {

    std::ifstream in_stream(file);
    std::string line, token;

    std::map<SuccinctGraph::AssocListKey, std::vector<SuccinctGraph::Assoc>>
        assoc_map;
    SuccinctGraph::AType atype;
    SuccinctGraph::Timestamp time;
    SuccinctGraph::NodeId src_id, dst_id;

    while (std::getline(in_stream, line)) {
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
            if (token_idx == 4) break;
        }
        SuccinctGraph::Assoc assoc =
            { dst_id, time, gen_random_string(bytes_per_attr) };
        assoc_map[std::make_pair(src_id, atype)].push_back(assoc);
    }
    in_stream.close();

    std::ofstream out_stream(out_file);
    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        auto src_id_and_atype = it->first;
        auto assoc_list = it->second;
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            auto assoc = *it2;
            assert(assoc.attr.length() == bytes_per_attr);
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
