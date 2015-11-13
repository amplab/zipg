#ifndef STRUCTURED_EDGE_TABLE_H
#define STRUCTURED_EDGE_TABLE_H

#include "SuccinctGraph.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class StructuredEdgeTable {
public:

    StructuredEdgeTable(const std::string edge_file = "")
        : edge_file_(edge_file)
    { }

    // Reads in the `edge_file_`, convert the data into structured data (i.e.
    // populate the `edges` map).
    void init(int option);

    // Limitation: we assume timestamp for a particular (src, atype) is
    // monotonically increasing for now (think: social network).
    //
    // Thread-safe for concurrent writes.
    void add_assoc(
        int64_t src,
        int64_t atype,
        int64_t dst,
        int64_t timestamp,
        const std::string& attr);

    std::vector<SuccinctGraph::Assoc> assoc_range(
        int64_t src,
        int64_t atype,
        int32_t off,
        int32_t len);

private:

    struct EdgeData {
        int64_t dst;
        int64_t timestamp;
        std::string attr;
    };

    // TODO: how inefficient is this?
    // Assumes the vector<EdgeData>'s are sorted by descending timestamps.
    std::unordered_map<
        int64_t, std::unordered_map<int64_t, std::vector<EdgeData> > > edges;

    const std::string edge_file_;

    std::mutex mutex_;
};

#endif
