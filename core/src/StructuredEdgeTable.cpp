#include "StructuredEdgeTable.h"

#include "GraphFormatter.hpp"
#include "utils.h"

#include <algorithm>

constexpr char SERDE_DELIM = '\x02';

void StructuredEdgeTable::construct() {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);

    COND_LOG_E("In StructuredEdgeTable::construct(), edge file '%s'\n",
        edge_file_.c_str());
    std::map<std::pair<int64_t, int64_t>,
        std::vector<SuccinctGraph::Assoc>> assoc_map;
    GraphFormatter::build_assoc_map(assoc_map, edge_file_);
    COND_LOG_E("Done building assoc map\n");

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
        return e1.timestamp < e2.timestamp;
    };

    for (auto it = edges.begin(); it != edges.end(); ++it) {
        auto& map = it->second;
        for (auto it2 = map.begin(); it2 != map.end(); ++it2) {
            std::sort(
                it2->second.begin(), it2->second.end(), cmp_by_decreasing_time);
        }
    }

    // Serialize
    std::ofstream oa(edge_file_ + "_logstore");
    size_t num_edges = 0;
    // boost::archive::text_oarchive boa(oa);

    // read class state from archive
    oa << edge_file_ << std::endl;

    for (auto it = edges.begin(); it != edges.end(); ++it) {
        int64_t key = it->first;
        auto& map = it->second;

        oa << key << SERDE_DELIM << map.size() << std::endl;

        for (auto it2 = map.begin(); it2 != map.end(); ++it2) {
            key = it2->first; // atype
            auto& vec = it2->second;

            oa << key << SERDE_DELIM << vec.size();
            for (auto& edge_data : vec) {
                oa << SERDE_DELIM << edge_data.dst
                    << SERDE_DELIM << edge_data.timestamp
                    << SERDE_DELIM << edge_data.attr;
            }
            oa << std::endl;

            num_edges += vec.size();
        }
    }
    LOG_E("StructuredEdgeTable wrote to '%s', %lld edges\n",
        (edge_file_ + "_logstore").c_str(), num_edges);

    num_edges_ = num_edges;
}

void StructuredEdgeTable::load() {
    boost::shared_lock<boost::shared_mutex> lk(mutex_);

    if (file_or_dir_exists((edge_file_ + "_logstore").c_str())) {
        std::string line, key, keysize, dst, timestamp, attr;
        int64_t src, atype;
        size_t num_edges = 0;
        edges.clear();

        std::ifstream ifs(edge_file_ + "_logstore");
        std::getline(ifs, line); // previous edge_file_ -> ignore!

        COND_LOG_E("StructuredEdgeTable: loading from '%s'\n",
            (edge_file_ + "_logstore").c_str());

        while (std::getline(ifs, line)) {
            std::stringstream ss(line);
            std::getline(ss, key, SERDE_DELIM);
            std::getline(ss, keysize);

            src = std::stoll(key);
            size_t size = std::stol(keysize);
            std::unordered_map<int64_t, std::vector<EdgeData>> map;
            std::vector<EdgeData> edge_datas;

            for (size_t i = 0; i < size; ++i) { // loop atypes for a src
                edge_datas.clear();

                std::getline(ifs, line);
                std::stringstream ss2(line);
                std::getline(ss2, key, SERDE_DELIM);
                std::getline(ss2, keysize, SERDE_DELIM);

                atype = std::stol(key);
                size_t vec_size = std::stol(keysize);

                for (size_t j = 0; j < vec_size; ++j) {
                    std::getline(ss2, dst, SERDE_DELIM);
                    std::getline(ss2, timestamp, SERDE_DELIM);
                    std::getline(ss2, attr, SERDE_DELIM);
                    COND_LOG_E("dst '%s', timestamp '%s', attr '%s', j %d\n",
                          dst.c_str(), timestamp.c_str(), attr.c_str(), j);
                    edge_datas.emplace_back(EdgeData{
                        std::stoll(dst), std::stoll(timestamp), attr });
                }
                map[atype] = edge_datas;
                num_edges += edge_datas.size();
            }

            edges[src] = std::move(map);
        }

        LOG_E("Loaded StructuredEdgeTable (log store), %lld edges\n",
            num_edges);

        num_edges_ = num_edges;
    }
}

void StructuredEdgeTable::add_assoc(
    int64_t src,
    int64_t dst,
    int64_t atype,
    int64_t timestamp,
    const std::string& attr)
{
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    edges[src][atype].emplace_back(EdgeData{ dst, timestamp, attr });
    ++num_edges_;
}

std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_range(
    int64_t src,
    int64_t atype,
    int32_t off,
    int32_t len)
{
    COND_LOG_E("GraphLogStore assoc_range(src = %lld, atype = %lld, off = %d, len = %d)\n",
        src, atype, off, len);

    std::vector<EdgeData> es;
    {
        boost::shared_lock<boost::shared_mutex> lk(mutex_);
        es = edges[src][atype];
    }
    std::vector<SuccinctGraph::Assoc> assocs;
    if (off >= es.size()) {
        return assocs;
    }
    for (size_t i = off;
        i < std::min(es.size(), static_cast<size_t>(off + len));
        ++i)
    {
        size_t j = es.size() - 1 - i; // backwards

        assocs.emplace_back();
        assocs.back() = SuccinctGraph::Assoc {
            src,
            es[j].dst,
            atype,
            es[j].timestamp,
            std::move(es[j].attr)
        };
    }
    return assocs;
}

// FIXME: scan for now...
std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_get(
    int64_t src,
    int64_t atype,
    const std::set<int64_t>& dst_id_set,
    int64_t t_low,
    int64_t t_high)
{
    COND_LOG_E("GraphLogStore assoc_get(src = %" PRId64 ", atype = %" PRId64 ","
        " dstIdSet = ..., tLow = %" PRId64 ", tHigh = %" PRId64 ")\n",
        src, atype, t_low, t_high);

    std::vector<EdgeData> es;
    {
        boost::shared_lock<boost::shared_mutex> lk(mutex_);
        es = edges[src][atype];
    }
    std::vector<SuccinctGraph::Assoc> assocs;

    for (auto it = es.rbegin(); it != es.rend(); ++it) {
        auto& edge_data = *it;
        if (edge_data.timestamp >= t_low
            && edge_data.timestamp <= t_high
            && dst_id_set.count(edge_data.dst) == 1)
        {
            assocs.emplace_back();
            assocs.back() = SuccinctGraph::Assoc {
                src,
                edge_data.dst,
                atype,
                edge_data.timestamp,
                std::move(edge_data.attr)
            };
        }
    }
    return assocs;
}

// FIXME: scan for now...
std::vector<SuccinctGraph::Assoc> StructuredEdgeTable::assoc_time_range(
    int64_t src,
    int64_t atype,
    int64_t t_low,
    int64_t t_high,
    int32_t len)
{
    COND_LOG_E("GraphLogStore assoc_time_range(src = %lld, atype = %lld, tLow = %lld, "
        "tHigh = %lld, len = %d)\n",
        src, atype, t_low, t_high, len);

    std::vector<SuccinctGraph::Assoc> assocs;
    if (len <= 0) {
        return assocs;
    }
    std::vector<EdgeData> es;
    {
        boost::shared_lock<boost::shared_mutex> lk(mutex_);
        es = edges[src][atype];
    }

    for (auto it = es.rbegin(); it != es.rend(); ++it) {
        auto& edge_data = *it;
        if (edge_data.timestamp >= t_low && edge_data.timestamp <= t_high) {
            assocs.emplace_back();
            assocs.back() = SuccinctGraph::Assoc {
                src,
                edge_data.dst,
                atype,
                edge_data.timestamp,
                std::move(edge_data.attr)
            };
            if (assocs.size() == len) {
                break;
            }
        }
    }
    return assocs;
}


void StructuredEdgeTable::build_backfill_edge_updates(
    std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
    int num_shards_to_mod)
{
    boost::shared_lock<boost::shared_mutex> lk(mutex_);

    edge_updates.clear();
    for (auto it = edges.begin(); it != edges.end(); ++it) {
        int64_t src = it->first;
        auto& atype_to_edges = it->second;

        auto& assoc_set = edge_updates[src % num_shards_to_mod];

        for (auto it2 = atype_to_edges.begin();
            it2 != atype_to_edges.end();
            ++it2)
        {
            assoc_set.emplace(src, it2->first);
        }
    }
    LOG_E("StructuredEdgeTable::build_backfill_edge_updates: %d shards\n",
        edge_updates.size());
}
