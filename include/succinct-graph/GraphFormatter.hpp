#ifndef GRAPH_FORMATTER_H
#define GRAPH_FORMATTER_H

#include <string>

// Formats input files into Succinct Graph-ready files.
class GraphFormatter {
public:

    // node_file: each row contains attributes (bytes) for the node
    //              whose ID is current row number - 1
    // output: "A.node" -> "A.node_formatted", each attr separated by
    //         pre-defined unique delimiters
    static void format_node_file(const std::string& node_file);

};

#endif
