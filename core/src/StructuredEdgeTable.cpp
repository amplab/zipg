#include "StructuredEdgeTable.h"

#include "GraphFormatter.hpp"

#include <algorithm>

void StructuredEdgeTable::init(int option) {
    std::map<std::pair<int64_t, int64_t>,
        std::vector<SuccinctGraph::Assoc>> assoc_map;
    GraphFormatter::build_assoc_map(assoc_map, edge_file_);

    edges.clear();
    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        int64_t src = (it->first).first, atype = (it->first).second;
        auto& list = edges[src][atype];
        auto& assocs = it->second;

        for (auto& assoc : assocs) {
            list.emplace_back();
            list.back() = EdgeData {
                assoc.dst_id,
                assoc.time,
                assoc.attr
            };
        }
    }

    auto cmp_by_decreasing_time = [](const EdgeData& e1, const EdgeData& e2) {
        return e1.timestamp > e2.timestamp;
    };

    for (auto it = edges.begin(); it != edges.end(); ++it) {
        auto& map = it->second;
        for (auto it2 = map.begin(); it2 != map.end(); ++it2) {
            std::sort(
                it2->second.begin(), it2->second.end(), cmp_by_decreasing_time);
        }
    }
}

void StructuredEdgeTable::add_assoc(
    int64_t src,
    int64_t atype,
    int64_t dst,
    int64_t timestamp,
    const std::string& attr)
{
    std::lock_guard<std::mutex> lock(mutex_);
    edges[src][atype].emplace_back(EdgeData{ dst, timestamp, attr });
}

std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_range(
    int64_t src,
    int64_t atype,
    int32_t off,
    int32_t len)
{
    std::vector<EdgeData> es(edges[src][atype]);
    std::vector<SuccinctGraph::Assoc> assocs;
    if (off >= es.size()) {
        return assocs;
    }
    for (size_t i = off;
        i < std::min(es.size(), static_cast<size_t>(off + len));
        ++i)
    {
        size_t j = es.size() - 1 - i;

        assocs.emplace_back();
        assocs.back() = SuccinctGraph::Assoc {
            src,
            atype,
            es[j].dst,
            es[j].timestamp,
            std::move(es[j].attr)
        };
    }
    return assocs;
}
