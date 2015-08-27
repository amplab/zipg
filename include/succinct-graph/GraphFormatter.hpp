#ifndef GRAPH_FORMATTER_H
#define GRAPH_FORMATTER_H

#include <map>
#include <string>
#include <vector>

#include "succinct-graph/SuccinctGraph.hpp"

// Formats input files into Succinct Graph-ready files.
class GraphFormatter {
public:

    // node_file: each row contains attributes (bytes) for the node
    //              whose ID is current row number - 1
    // output: "A.node" -> "A.node_formatted", each attr separated by
    //         pre-defined unique delimiters
    static void format_node_file(const std::string& node_file);

    // Outputs random node table, where each line's attributes are properly
    // separated by unique delimiters.
    static void create_random_node_table(
        const std::string& out_file,
        int num_nodes,
        int num_attr,
        int freq,
        int len);

    // Outputs a node table file suitable for use in SuccinctGraph::construct().
    // Each node's attributes are taken from `attr_file`, and are properly
    // separated by unique delimiters (specified in SuccinctGraph::DELIMITERS).
    //
    // Asserts that any internal delimiters used do not appear in attr contents.
    static void create_node_table(
        const std::string& out_file,
        const std::string& attr_file,
        int num_nodes,
        int num_attr,
        int freq,
        int len);

    // Each input line is of the form
    //     "srcId<edge_inner_delim>dstId<edge_end_delim>",
    // where the inner delimiter defaults to a whitespace and the end delimiter
    // defaults to an end-of-line.
    //
    // The input edge list file can contain comment lines that start with '#',
    // all of which will be ignored.
    //
    // For each edge, we generate atype and timestamp uniformly at
    // random from some range.
    //
    // Edge attributes are taken from `attr_file`, with truncation/padding so
    // that each attribute has specified length.  The output edge file can be
    // fed into SuccinctGraph::construct().
    //
    // If `min_out_degree` is not -1, augment a node's out-neighbor list
    // randomly if its out-degree is less than `min_out_degree`.  These dummy
    // edges are assigned empty edge attribute and a timestamp of 0. The
    // function also tries its best to have these dummy edges distinct
    // from real, existing ones (hence no parallel edges).
    static void create_edge_table(
        const std::string& file,
        const std::string& attr_file,
        const std::string& out_file,
        int bytes_per_attr,
        char edge_inner_delim = ' ',
        char edge_end_delim = '\n',
        int num_atype = 5,
        int min_out_degree = -1);

    // Each input line is of the form
    //     "srcId<edge_inner_delim>dstId<edge_end_delim>",
    // where the inner delimiter defaults to a whitespace and the end delimiter
    // defaults to an end-of-line.
    //
    // The input edge list file can contain comment lines that start with '#',
    // all of which will be ignored.
    static std::vector<std::vector<int64_t>> read_edge_list(
        const std::string& file,
        char edge_inner_delim = ' ',
        char edge_end_delim = '\n');

    // Hybrid of read_edge_list().  Input is of the form:
    //     "srcId <edge_inner_delim_between_ids>
    //      dstId <edge_inner_delim_after_ids>
    //      atype <edge_delim_after_atype>
    //      timestamp <edge_delim_after_time> ..."
    // on each line.
    static std::map<std::pair<int64_t, int64_t>, std::vector<int64_t>>
    read_assoc_list(
        const std::string& file,
        bool read_dst_ids, // true if read dst ids, else read timestamps
        char edge_inner_delim_between_ids = ' ',
        char edge_inner_delim_after_ids = ' ',
        char edge_delim_after_atype = ' ',
        char edge_delim_after_time = ' ');

    // Variant that reads all assoc data in entirety.
    static std::map<SuccinctGraph::AssocListKey,
                    std::vector<SuccinctGraph::Assoc>>
    read_assoc_list(
        const std::string& file,
        char edge_inner_delim_between_ids = ' ',
        char edge_inner_delim_after_ids = ' ',
        char edge_delim_after_atype = ' ',
        char edge_delim_after_time = ' ');

    // Applies special delimiter logic: prepend each attribute with a unique
    // delimiter that doesn't appear in the input; concatenates these into a
    // string, then takes care of appending delimiters for absent attributes.
    // Concatenates the result strings with new lines (including at the end).
    // The output is suitable to be used in SuccinctGraph::construct().
    static std::string format_node_attrs_str(
        std::vector<std::vector<std::string>> node_attrs);

    // Output: nodeId [delim] attr0 [delim] ...
    // Note that node ids must be exactly the range [0, ..., L].
    static void format_neo4j_node_from_node_file(
        const std::string& delimed_node_file,
        const std::string& neo4j_node_out,
        char neo4j_delim = '\x02');

    // Output: srcId [delim] dstId [delim] atype [delim] timestamp [delim] attr
    static void format_neo4j_edge_from_edge_file(
        const std::string& delimed_edge_file,
        const std::string& neo4j_edge_out,
        char neo4j_delim = '\x02');

    // Used only when generating & parsing queries, not part of the internal
    // graph layout.  Assumes this is char uniquely identifiable (among attrs).
    static const char QUERY_FILED_DELIM = '\x02';

};

#endif
