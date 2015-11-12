#include "StructuredEdgeTable.h"

#include <algorithm>

void StructuredEdgeTable::add_assoc(
    int64_t src,
    int64_t atype,
    int64_t dst,
    int64_t timestamp,
    const std::string& attr)
{
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
