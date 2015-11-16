#include "StructuredEdgeTable.h"

#include "GraphFormatter.hpp"
#include "utils.h"

#include <algorithm>

void StructuredEdgeTable::construct() {
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

    // Serialize
    std::ofstream oa(edge_file_ + "_logstore");
    size_t num_edges = 0;
    // boost::archive::text_oarchive boa(oa);

    // read class state from archive
    oa << edge_file_ << std::endl;

    for (auto it = edges.begin(); it != edges.end(); ++it) {
        int64_t key = it->first;
        auto& map = it->second;

        oa << key << ' ' << map.size() << std::endl;

        for (auto it2 = map.begin(); it2 != map.end(); ++it2) {
            key = it2->first; // atype
            auto& vec = it2->second;

            oa << key << ' ' << vec.size();
            for (auto& edge_data : vec) {
                oa << ' ' << edge_data.dst
                    << ' ' << edge_data.timestamp
                    << ' ' << edge_data.attr;
            }
            oa << std::endl;

            num_edges += vec.size();
        }
    }
    LOG_E("StructuredEdgeTable wrote to '%s', %lld edges\n",
        (edge_file_ + "_logstore").c_str(), num_edges);
}

void StructuredEdgeTable::load() {
    if (file_or_dir_exists((edge_file_ + "_logstore").c_str())) {
        std::string line, key, keysize, dst, timestamp, attr;
        int64_t src, atype;
        size_t num_edges = 0;
        edges.clear();

        std::ifstream ifs(edge_file_ + "_logstore");
        std::getline(ifs, edge_file_); // edge_file_

        COND_LOG_E("StructuredEdgeTable: loading from '%s'\n",
            (edge_file_ + "_logstore").c_str());

        while (std::getline(ifs, line)) {
            std::stringstream ss(line);
            std::getline(ss, key, ' ');
            std::getline(ss, keysize);

            src = std::stoll(key);
            size_t size = std::stol(keysize);
            std::unordered_map<int64_t, std::vector<EdgeData>> map;
            std::vector<EdgeData> edge_datas;

            for (size_t i = 0; i < size; ++i) { // loop atypes for a src
                edge_datas.clear();

                std::getline(ifs, line);
                std::stringstream ss2(line);
                std::getline(ss, key, ' ');
                std::getline(ss, keysize, ' ');

                atype = std::stol(key);
                size_t vec_size = std::stol(keysize);

                for (size_t j = 0; j < vec_size; ++j) {
                    std::getline(ss2, dst, ' ');
                    std::getline(ss2, timestamp, ' ');
                    std::getline(ss2, attr, ' ');
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
