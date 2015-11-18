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

    // FIXME: what's the input?
    edge_table_ = std::make_shared<FileSuffixStore>(edge_file);
    edge_table_->construct();
}

void GraphSuffixStore::construct() {
    construct(node_file_, edge_file_);
}

void GraphSuffixStore::load() {
    node_table_ = std::make_shared<KVSuffixStore>(node_file_);
    node_table_->load();
    COND_LOG_E("GraphSuffixStore: node table loaded\n");

    edge_table_ = std::make_shared<FileSuffixStore>(edge_file_);
    edge_table_->load();
    COND_LOG_E("GraphSuffixStore: edge table loaded\n");
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
    COND_LOG_E("src %lld atype %d, %d offsets\n", src, atype, offs.size());
    assert(offs.size() <= 1);

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt;
    std::string str;
    std::vector<SuccinctGraph::Assoc> result;

    for (int64_t curr_off : offs) {
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

int64_t GraphSuffixStore::assoc_count(int64_t src, int64_t atype) {
    std::vector<int64_t> offs;
    edge_table_->search(
        offs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    assert(offs.size() <= 1);
    if (offs.size() == 0) {
        return 0;
    }

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt;
    std::string str;
    std::vector<SuccinctGraph::Assoc> result;

    int64_t curr_off = offs[0];

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
    return std::stoll(str);
}

int GraphSuffixStore::time_range_binary_search_lower_bound(
    int64_t t_low,
    int64_t cnt,
    int64_t curr_off,
    std::string& tmp_token,
    const int32_t timestamp_width)
{
    int l = 0, r = cnt, m;
    int64_t ts;

    // check t_l >= t_low
    edge_table_->extract(tmp_token, curr_off, timestamp_width);
    ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
    if (ts < t_low) {
        return -1;
    }

    // binary search: locates smallest t s.t. t >= t_low
    // invariant: target in [l, r)
    while (l + 1 < r) {
        m = (l + r) / 2;
        edge_table_->extract(
            tmp_token,
            curr_off + m * timestamp_width,
            timestamp_width);
        ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
        if (ts >= t_low) {
            l = m; // note timestamps are decreasing
        }
        else {
            r = m;
        }
    }
    assert(l + 1 == r);
    return l;
}

int GraphSuffixStore::time_range_binary_search_upper_bound(
    int64_t t_high,
    int64_t cnt,
    int64_t curr_off,
    std::string& tmp_token,
    const int32_t timestamp_width)
{
    int l = -1, r = cnt - 1, m;
    int64_t ts;

    // check t_r <= t_high
    edge_table_->extract(
        tmp_token, curr_off + r * timestamp_width, timestamp_width);
    ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
    if (ts > t_high) {
        return -1;
    }

    while (l + 1 < r) {
        m = (l + r) / 2;
        edge_table_->extract(
            tmp_token, curr_off + m * timestamp_width, timestamp_width);
        ts = SuccinctGraphSerde::decode_timestamp(tmp_token);
        if (ts > t_high) {
            l = m;
        } else {
            r = m;
        }
    }
    assert(l + 1 == r);
    return r;
}

std::vector<SuccinctGraph::Assoc> GraphSuffixStore::assoc_time_range(
    int64_t src,
    int64_t atype,
    int64_t t_low,
    int64_t t_high,
    int32_t len)
{
    COND_LOG_E("GraphSuffixStore: assoc_time_range(src = %lld, atype = %lld, "
        "tLow = %lld, tHigh = %lld, len = %d)\n",
        src, atype, t_low, t_high, len);

    std::vector<int64_t> eoffs;
    edge_table_->search(
        eoffs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    std::vector<SuccinctGraph::Assoc> result;
    std::string str;

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt;

    int32_t len_saved = len;

    for (int64_t curr_off : eoffs) {
        COND_LOG_E("edge table offset = %llu\n", curr_off);

        curr_off = edge_table_->skip_until(
            curr_off, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);

        edge_table_->extract(
            str,
            curr_off,
            SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
        COND_LOG_E("extracted timestamp width = '%s'\n", str.c_str());
        timestamp_width = std::stoi(str);

        edge_table_->extract(
            str,
            curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
            SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
        COND_LOG_E("extracted dst id width = '%s'\n", str.c_str());
        dst_id_width = std::stoi(str);

        curr_off = edge_table_->extract_until(
            str,
            curr_off +
                SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED +
                SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
            SuccinctGraph::EDGE_WIDTH_DELIM);
        COND_LOG_E("extracted cnt = '%s'\n", str.c_str());
        cnt = std::stoll(str);

        curr_off = edge_table_->extract_until(
            str, curr_off, SuccinctGraph::METADATA_DELIM);
        edge_width = std::stoi(str);
        COND_LOG_E("extracted edge width = '%s'\n", str.c_str());

        int range_left, range_right; // in-range: [left, right]

        range_right = time_range_binary_search_lower_bound(
            t_low, cnt, curr_off, str, timestamp_width);
        if (range_right == -1) {
            continue;
        }

        // binary search: locates largest t s.t. t <= t_high
        // invariant: target in (l, r]
        range_left = time_range_binary_search_upper_bound(
            t_high, cnt, curr_off, str, timestamp_width);
        if (range_left == -1) {
            continue;
        }

        // limit to first `len` edges
        range_right = std::min(range_right, range_left + len - 1);

        COND_LOG_E("range left: %d, range right: %d, cnt: %lld\n",
            range_left, range_right, cnt);
        if (range_left > range_right) {
            continue;
        }

        // extract in-range timestamps
        edge_table_->extract(
            str,
            curr_off + range_left * timestamp_width,
            (range_right - range_left + 1) * timestamp_width);
        COND_LOG_E("extracted timestamps = '%s'\n", str.c_str());
        std::vector<int64_t> decoded_timestamps =
            SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

        // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
        curr_off += cnt * timestamp_width;
        edge_table_->extract(
            str,
            curr_off + range_left * dst_id_width,
            (range_right - range_left + 1) * dst_id_width);
        COND_LOG_E("extracted dst ids: '%s'\n", str.c_str());
        std::vector<int64_t> decoded_dst_ids =
            SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

        // TODO: another choice is to do a single extract then filter; evaluate?
        // Now extract only the in-set (and in-range) attrs
        curr_off += cnt * dst_id_width;
        for (size_t i = 0; i < decoded_timestamps.size(); ++i) {
            result.emplace_back();
            // decoded dst ids and timestamps start w/ absolute idx range_left
            result.back().src_id = src;
            result.back().dst_id = decoded_dst_ids[i];
            result.back().atype = atype;
            result.back().time = decoded_timestamps[i];
            edge_table_->extract(
                result.back().attr,
                curr_off + (range_left + i) * edge_width,
                edge_width);
        }
    }
    return result;
}

void GraphSuffixStore::obj_get(std::vector<std::string>& result, int64_t obj_id) {
    LOG_E("obj_get(%lld)\n", obj_id);
    assert(false && "benchmark should not have routed the query here!");
}

std::vector<SuccinctGraph::Assoc> GraphSuffixStore::assoc_get(
    int64_t src,
    int64_t atype,
    const std::set<int64_t>& dst_id_set,
    int64_t t_low,
    int64_t t_high)
{
    COND_LOG_E("GraphSuffixStore assoc_get(src = %" PRId64 ", "
        "atype = %" PRId64 ","
        " dstIdSet = ..., tLow = %" PRId64 ", tHigh = %" PRId64 ")\n",
        src, atype, t_low, t_high);

    std::vector<int64_t> eoffs;
    edge_table_->search(
        eoffs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    std::vector<SuccinctGraph::Assoc> result;
    std::string str;

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt;

    for (int64_t curr_off : eoffs) {
        COND_LOG_E("edge table offset = %llu\n", curr_off);

        // skip after src, atype
        curr_off = edge_table_->skip_until(
            curr_off, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);

        edge_table_->extract(
            str,
            curr_off,
            SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
        COND_LOG_E("extracted timestamp width = '%s'\n", str.c_str());
        timestamp_width = std::stoi(str);

        edge_table_->extract(
            str,
            curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
            SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
        COND_LOG_E("extracted dst id width = '%s'\n", str.c_str());
        dst_id_width = std::stoi(str);

        curr_off = edge_table_->extract_until(
            str,
            curr_off +
                SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED +
                SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
            SuccinctGraph::EDGE_WIDTH_DELIM);
        COND_LOG_E("extracted cnt = '%s'\n", str.c_str());
        cnt = std::stoll(str);

        curr_off = edge_table_->extract_until(
            str, curr_off, SuccinctGraph::METADATA_DELIM);
        edge_width = std::stoi(str);
        COND_LOG_E("extracted edge width = '%s'\n", str.c_str());

        int range_left, range_right; // in-range: [left, right]

        range_right = time_range_binary_search_lower_bound(
            t_low, cnt, curr_off, str, timestamp_width);
        if (range_right == -1) {
            continue;
        }

        // binary search: locates largest t s.t. t <= t_high
        // invariant: target in (l, r]
        range_left = time_range_binary_search_upper_bound(
            t_high, cnt, curr_off, str, timestamp_width);
        if (range_left == -1) {
            continue;
        }

        COND_LOG_E("range left: %d, range right: %d, cnt: %lld\n",
            range_left, range_right, cnt);
        if (range_left > range_right) {
            continue;
        }

        // extract in-range timestamps
        edge_table_->extract(
            str,
            curr_off + range_left * timestamp_width,
            (range_right - range_left + 1) * timestamp_width);
        COND_LOG_E("extracted timestamps = '%s'\n", str.c_str());
        std::vector<int64_t> decoded_timestamps =
            SuccinctGraphSerde::decode_multi_timestamps(str, timestamp_width);

        // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
        curr_off += cnt * timestamp_width;
        edge_table_->extract(
            str,
            curr_off + range_left * dst_id_width,
            (range_right - range_left + 1) * dst_id_width);
        COND_LOG_E("extracted dst ids: '%s'\n", str.c_str());

        std::vector<int64_t> decoded_dst_ids =
            SuccinctGraphSerde::decode_multi_node_ids(str, dst_id_width);

        // filter
        std::vector<int64_t> in_set_indexes;
        for (size_t i = 0; i < decoded_dst_ids.size(); ++i) {
            COND_LOG_E("decoded_dst_id[i=%d] = %lld\n", i, decoded_dst_ids[i]);

            if (dst_id_set.count(decoded_dst_ids[i]) != 0) {
                in_set_indexes.push_back(range_left + i);
                COND_LOG_E("pass filter id: %d, decoded_dst_id[i] = %lld\n",
                    range_left + i, decoded_dst_ids[i]);
            }
        }

        // TODO: another choice is to do a single extract then filter; evaluate?
        // Now extract only the in-set (and in-range) attrs
        curr_off += cnt * dst_id_width;
        for (int64_t idx : in_set_indexes) {
            result.emplace_back();
            // decoded dst ids and timestamps start w/ absolute idx range_left
            result.back().src_id = src;
            result.back().dst_id = decoded_dst_ids[idx - range_left];
            result.back().atype = atype;
            result.back().time = decoded_timestamps[idx - range_left];
            edge_table_->extract(
                result.back().attr, curr_off + idx * edge_width, edge_width);
        }
    }
    return result;
}

void GraphSuffixStore::build_backfill_edge_updates(
    std::unordered_map<int, GraphFormatter::AssocSet>& edge_updates,
    int num_shards_to_mod)
{
    GraphFormatter::build_edge_updates(
        edge_updates, edge_file_, num_shards_to_mod);

    LOG_E("GraphSuffixStore::build_backfill_edge_updates: %d shards\n",
        edge_updates.size());
}
