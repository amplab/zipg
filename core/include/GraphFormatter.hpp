#ifndef GRAPH_FORMATTER_H
#define GRAPH_FORMATTER_H

#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "SuccinctGraph.hpp"

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

    static void create_node_table_zipf(
        const std::string& out_file,
        const std::string& attr_file,
        int num_nodes,
        int num_attr,
        int len,
        int corpus_size,
        bool report_freq_dist = true);

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
    // randomly if its out-degree is less than `min_out_degree`.  By default,
    // these dummy edges are assigned empty edge attribute and a timestamp of 0.
    // This behavior can be overriden by setting `assign_ts_attr_to_dummy_edges`
    // to true.  This function also tries its best to have these dummy edges
    // distinct from the real, existing ones (hence no parallel edges).
    static void create_edge_table(
        const std::string& file,
        const std::string& attr_file,
        const std::string& out_file,
        int bytes_per_attr,
        char edge_inner_delim = ' ',
        char edge_end_delim = '\n',
        int num_atype = 5,
        int min_out_degree = -1,
        bool assign_ts_attr_to_dummy_edges = false);

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

    // Applies special delimiter logic: prepend each attribute with a unique
    // delimiter that doesn't appear in the input; concatenates these into a
    // string, then takes care of appending delimiters for absent attributes.
    // Concatenates the result strings with new lines (including at the end).
    // The output is suitable to be used in SuccinctGraph::construct().
    static std::string format_node_attrs_str(
        const std::vector<std::vector<std::string>>& node_attrs);

    // Given a properly delimed property string (e.g. returned by
    // format_node_attrs_str()), attach the metadata length info.  The result
    // is suitable for acting as a value in the Node Table.
    static std::string attach_attr_lengths(const std::string& delimed);

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

    static void make_rand_assoc(
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
        bool augmented_assoc = false,
        bool assign_ts_attr_to_dummy = false);

    // Used only when generating & parsing queries, not part of the internal
    // graph layout.  Assumes this is char uniquely identifiable (among attrs).
    static const char QUERY_FILED_DELIM = '\x02';

private:

    static int64_t time_millis() {
        struct timeval now;
        gettimeofday(&now, NULL);
        return (int64_t) now.tv_sec * 1000 + now.tv_usec / 1000;
    }

    // This can be > 5x faster (loop unroll / static lookup).
    static int32_t num_digits(int64_t number) {
       if (number == 0) return 1;
       int32_t digits = 0;
       while (number != 0) {
           number /= 10;
           ++digits;
       }
       return digits;
    }

    static void output_node_attributes(
        const std::string& out_file,
        const std::vector<std::vector<std::string>>& attributes,
        int num_nodes,
        int num_attr)
    {
        std::ofstream s_out(out_file);
        std::vector<std::string> node_attrs;
        for (int i = 0; i < num_nodes; i++) {
            node_attrs.clear();
            for (int attr = 0; attr < num_attr; attr++) {
                // weak assert
                assert(attributes[attr][i].find(static_cast<char>(
                    SuccinctGraph::DELIMITERS[attr])) == std::string::npos);
                node_attrs.push_back(attributes[attr][i]);
            }
            s_out << GraphFormatter::format_node_attrs_str({ node_attrs });
        }
        s_out.close();
    }

};

#endif
