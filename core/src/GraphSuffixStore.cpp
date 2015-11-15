#include "GraphSuffixStore.h"

#include "GraphFormatter.hpp"
#include "SuccinctGraphSerde.hpp"

void GraphSuffixStore::construct(
    const std::string& node_file,
    const std::string& edge_file)
{
    node_file_ = node_file;
    edge_file_ = edge_file;

    node_table_ = std::make_shared<KVSuffixStore>(node_file);
    node_table_->construct();

    edge_table_ = std::make_shared<FileSuffixStore>(edge_file);
    edge_table_->construct();
}

void GraphSuffixStore::construct() {
    construct(node_file_, edge_file_);
}

void GraphSuffixStore::load() {
    node_table_ = std::make_shared<KVSuffixStore>(node_file_);
    node_table_->load();

    edge_table_ = std::make_shared<FileSuffixStore>(edge_file_);
    edge_table_->load();
}

void GraphSuffixStore::get_attribute(
    std::string& result, int64_t node_id, int attr)
{
    result.clear();
    std::string str;
    assert(attr < SuccinctGraph::MAX_NUM_NODE_ATTRS);
    node_table_->get_value(str, node_id); // TODO: handle non-existent?

    // get the initial dist first
    int i = 0, dist = 0;
    while (str[i] != SuccinctGraph::NODE_TABLE_HEADER_DELIM) {
        dist = dist * 10 + (str[i] - '0');
        ++i;
    }
    ++i;
    int init_dist = dist;

    dist += attr + 1; // skip past the delims as well
    for (int j = 1; j <= attr; ++j) {
        int tmp = 0;
        while (str[i] != SuccinctGraph::NODE_TABLE_HEADER_DELIM) {
            tmp = tmp * 10 + (str[i] - '0');
            ++i;
        }
        ++i;
        dist += tmp;
    }

    // also skip the dist and its delim as well
    dist += SuccinctGraph::num_digits(init_dist) + 1;

    while (str[dist] !=
        static_cast<char>(SuccinctGraph::DELIMITERS[attr + 1]))
    {
        result += str[dist]; // TODO: probably exists a better way to write this
        ++dist;
    }
}

void GraphSuffixStore::get_nodes(
    std::set<int64_t>& result,
    int attr,
    const std::string& search_key)
{
    result.clear();
    node_table_->search(result,
        std::move(SuccinctGraph::mk_node_attr_key(attr, search_key)));
}

void GraphSuffixStore::get_nodes(
    std::set<int64_t>& result,
    int attr1,
    const std::string& search_key1,
    int attr2,
    const std::string& search_key2)
{
    result.clear();
    std::set<int64_t> s1, s2;
    node_table_->search(s1,
        std::move(SuccinctGraph::mk_node_attr_key(attr1, search_key1)));
    node_table_->search(s2,
        std::move(SuccinctGraph::mk_node_attr_key(attr2, search_key2)));

    // result.end() is a hint that supposedly is faster than .begin()
    std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                          std::inserter(result, result.end()));
}

std::vector<SuccinctGraph::Assoc> GraphSuffixStore::assoc_range(
    int64_t src,
    int64_t atype,
    int32_t off,
    int32_t len)
{
    std::vector<int64_t> offs;
    edge_table_->search(
        offs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    assert(offs.size() <= 1);

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt;
    std::string str;
    std::vector<SuccinctGraph::Assoc> result;

    for (int64_t curr_off : offs) {
        // TODO: extract stuff, put into assocs
        // skip after src, atype
        curr_off = edge_table_->skip_until(
            curr_off, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);

        edge_table_->extract(str,
            curr_off, SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
        COND_LOG_E("extracted timestamp width = '%s'\n", str.c_str());
        timestamp_width = std::stoi(str);
        curr_off += SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED;

        edge_table_->extract(str,
            curr_off,
            SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
        COND_LOG_E("extracted dst id width = '%s'\n", str.c_str());
        dst_id_width = std::stoi(str);

        curr_off += SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED;
        curr_off = edge_table_->extract_until(
            str, curr_off, SuccinctGraph::EDGE_WIDTH_DELIM);
        COND_LOG_E("extracted cnt = '%s'\n", str.c_str());
        cnt = std::stoll(str);

        curr_off = edge_table_->extract_until(
            str, curr_off, SuccinctGraph::METADATA_DELIM);
        edge_width = std::stoi(str);
        COND_LOG_E("extracted edge width = '%s'\n", str.c_str());

        len = std::min(static_cast<int64_t>(len), cnt - off);
        if (len <= 0) {
            continue;
        }

        edge_table_->extract(
            str,
            curr_off + off * timestamp_width,
            len * timestamp_width);

        std::vector<int64_t> decoded_timestamps =
            SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

        COND_LOG_E("extracted timestamps = '%s'\n", str.c_str());

        curr_off += cnt * timestamp_width;
        edge_table_->extract(
            str,
            curr_off + off * dst_id_width,
            len * dst_id_width);

        std::vector<int64_t> decoded_dst_ids =
            SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

        COND_LOG_E("extracted dst ids: '%s'\n", str.c_str());

        curr_off += cnt * dst_id_width;
        edge_table_->extract(
            str,
            curr_off + off * edge_width,
            len * edge_width);

        COND_LOG_E("extracted attrs = '%s'\n", str.c_str());

        // Perf comparisons:
        // https://goo.gl/B3Wvj1 - naive
        // https://goo.gl/ckAnB0 - add ctor to Assoc struct, emplace_back w/ it
        // https://goo.gl/zcLovO - don't add ctor, emplace_back() no arg
        for (size_t i = 0; i < decoded_timestamps.size(); ++i) {
            result.emplace_back();
            result.back().src_id = src;
            result.back().dst_id = decoded_dst_ids[i];
            result.back().atype = atype;
            result.back().time = decoded_timestamps[i];
            result.back().attr = std::move(
                str.substr(i * edge_width, edge_width));
        }
    }
    return result;
}
