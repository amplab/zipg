#ifndef STRUCTURED_EDGE_TABLE_H
#define STRUCTURED_EDGE_TABLE_H

#include "SuccinctGraph.hpp"

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

//#include <boost/archive/text_oarchive.hpp>
//#include <boost/archive/text_iarchive.hpp>
//#include <boost/serialization/string.hpp>
//#include <boost/serialization/version.hpp>

class StructuredEdgeTable {
public:

    StructuredEdgeTable(const std::string edge_file = "")
        : edge_file_(edge_file)
    { }

    // Reads in the `edge_file_`, convert the data into structured data (i.e.
    // populate the `edges` map).
    void construct();

    void load();

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

    inline int64_t assoc_count(int64_t src, int64_t atype) {
        return edges[src][atype].size();
    }

//    template<class Archive>
//    void serialize(Archive & ar, const unsigned int version) {
//        // read class state from archive
//        ar & edge_file_;
//        for (auto it = edges.begin(); it != edges.end(); ++it) {
//            int64_t key = it->first;
//            auto& map = it->second;
//            ar & key;
//            for (auto it2 = map.begin(); it2 != map.end(); ++it2) {
//                key = it2->first; // atype
//                auto& vec = it2->second;
//                ar & key;
//                for (auto& edge_data : vec) {
//                    ar & edge_data.dst;
//                    ar & edge_data.timestamp;
//                    ar & edge_data.attr;
//                }
//            }
//        }
//    }

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

    std::string edge_file_;

    std::mutex mutex_;
};

#endif
