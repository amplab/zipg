#ifndef GRAPH_FORMATTER_H
#define GRAPH_FORMATTER_H

#include <string>
#include <vector>

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

    // If `has_atype_timestamp` is false, then each line is "srcId dstId", and
    // we generate atype and timestamp uniformly at random from some range.
    // Otherwise, each line is of the form "176481 2417 1341102251 MT" (map
    // MT->0, RE->1, RT->2 as atypes), which is Higgs-Twitter specific.
    //
    // The input edge list file can contain comment lines that start with '#',
    // so that these lines will be ignored.
    //
    // Edge attributes are taken from `attr_file`, with truncation/padding so
    // that each attribute has specified length.  The output edge file can be
    // fed into SuccinctGraph::construct().
    static void create_edge_table(
        const std::string& file,
        const std::string& attr_file,
        const std::string& out_file,
        int bytes_per_attr,
        int num_atype = 5,
        bool has_atype_timestamp = false);

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
