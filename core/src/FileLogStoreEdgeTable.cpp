#include "FileLogStoreEdgeTable.h"
#include "SuccinctGraphSerde.hpp"

std::vector<SuccinctGraph::Assoc> FileLogStoreEdgeTable::assoc_range(
    int64_t src,
    int64_t atype,
    int32_t off,
    int32_t len)
{
    std::vector<int64_t> offs;
    file_log_store_.search(
        offs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    COND_LOG_E("src %lld atype %d, %d offsets\n", src, atype, offs.size());

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt, curr_off;
    std::string str;
    std::vector<SuccinctGraph::Assoc> result;

    if (offs.empty()) {
        return result;
    }

    // Reversed, by the assumption that time is monotonically increasing
    // Also, off & len are handled here, since we're dealing w/ singleton lists
    int32_t curr_len = 0;
    for (auto it = std::min(offs.rend(), offs.rbegin() + off);
         it != offs.rend() && curr_len < len;
         ++it)
    {
        curr_off = *it;
        COND_LOG_E("curr off = %lld\n", curr_off);

        // skip after src, atype
        curr_off = file_log_store_.skip_until(
            curr_off, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);

        file_log_store_.extract(str,
            curr_off, SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
        COND_LOG_E("extracted timestamp width = '%s'\n", str.c_str());
        timestamp_width = std::stoi(str);
        curr_off += SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED;

        file_log_store_.extract(str,
            curr_off,
            SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
        COND_LOG_E("extracted dst id width = '%s'\n", str.c_str());
        dst_id_width = std::stoi(str);

        curr_off += SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED;
        curr_off = file_log_store_.extract_until(
            str, curr_off, SuccinctGraph::EDGE_WIDTH_DELIM);
        COND_LOG_E("extracted cnt = '%s'\n", str.c_str());
        cnt = std::stoll(str);
        assert(cnt == 1 && "cnt != 1 in FileLogStoreEdgeTable!");

        curr_off = file_log_store_.extract_until(
            str, curr_off, SuccinctGraph::METADATA_DELIM);
        edge_width = std::stoi(str);
        COND_LOG_E("extracted edge width = '%s'\n", str.c_str());

        // Note: in extraction of timestamps, dst ids, and attrs, we again
        // make use of the assumption that the assoc lists here are singleton.
        file_log_store_.extract(str, curr_off, timestamp_width);

        int64_t decoded_timestamp = SuccinctGraphSerde::decode_timestamp(str);

        COND_LOG_E("extracted timestamps = '%s'\n", str.c_str());

        curr_off += timestamp_width;
        file_log_store_.extract(str, curr_off, dst_id_width);

        int64_t decoded_dst_id = SuccinctGraphSerde::decode_node_id(str);

        COND_LOG_E("extracted dst ids: '%s'\n", str.c_str());

        curr_off += dst_id_width;
        file_log_store_.extract(str, curr_off, edge_width);

        COND_LOG_E("extracted attrs = '%s'\n", str.c_str());

        result.emplace_back();
        result.back().src_id = src;
        result.back().dst_id = decoded_dst_id;
        result.back().atype = atype;
        result.back().time = decoded_timestamp;
        result.back().attr = std::move(str);

        ++curr_len;
    }
    return result;
}

// Performs linear scan to execute the range filter.
std::vector<SuccinctGraph::Assoc> FileLogStoreEdgeTable::assoc_get(
    int64_t src,
    int64_t atype,
    const std::set<int64_t>& dst_id_set,
    int64_t t_low,
    int64_t t_high)
{
    COND_LOG_E("FileLogStoreEdgeTable assoc_get(src = %" PRId64 ", "
        "atype = %" PRId64 ","
        " dstIdSet = ..., tLow = %" PRId64 ", tHigh = %" PRId64 ")\n",
        src, atype, t_low, t_high);

    std::vector<int64_t> eoffs;
    file_log_store_.search(
        eoffs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    std::vector<SuccinctGraph::Assoc> result;
    std::string str;

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt, curr_off;

    if (t_low > t_high) {
        return result;
    }

    for (auto it = eoffs.rbegin(); it != eoffs.rend(); ++it) {
        curr_off = *it;
        COND_LOG_E("edge table offset = %llu\n", curr_off);

        // skip after src, atype
        curr_off = file_log_store_.skip_until(
            curr_off, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);

        file_log_store_.extract(
            str,
            curr_off,
            SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
        COND_LOG_E("extracted timestamp width = '%s'\n", str.c_str());
        timestamp_width = std::stoi(str);

        file_log_store_.extract(
            str,
            curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
            SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
        COND_LOG_E("extracted dst id width = '%s'\n", str.c_str());
        dst_id_width = std::stoi(str);

        curr_off = file_log_store_.extract_until(
            str,
            curr_off +
                SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED +
                SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
            SuccinctGraph::EDGE_WIDTH_DELIM);
        COND_LOG_E("extracted cnt = '%s'\n", str.c_str());
        cnt = std::stoll(str);
        assert(cnt == 1);

        curr_off = file_log_store_.extract_until(
            str, curr_off, SuccinctGraph::METADATA_DELIM);
        edge_width = std::stoi(str);
        COND_LOG_E("extracted edge width = '%s'\n", str.c_str());

        file_log_store_.extract(
            str,
            curr_off,
            timestamp_width);
        COND_LOG_E("extracted timestamps = '%s'\n", str.c_str());
        int64_t decoded_timestamp = SuccinctGraphSerde::decode_timestamp(str);

        if (decoded_timestamp < t_low) {
            break;
        }
        if (decoded_timestamp > t_high) {
            continue;
        }

        curr_off += timestamp_width;
        file_log_store_.extract(str, curr_off, dst_id_width);
        COND_LOG_E("extracted dst ids: '%s'\n", str.c_str());
        int64_t decoded_dst_id = SuccinctGraphSerde::decode_node_id(str);
        if (dst_id_set.count(decoded_dst_id) == 0) {
            continue;
        }

        curr_off += dst_id_width;
        result.emplace_back();
        result.back().src_id = src;
        result.back().dst_id = decoded_dst_id;
        result.back().atype = atype;
        result.back().time = decoded_timestamp;
        file_log_store_.extract(result.back().attr, curr_off, edge_width);
    }
    return result;
}

std::vector<SuccinctGraph::Assoc> FileLogStoreEdgeTable::assoc_time_range(
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
    file_log_store_.search(
        eoffs, SuccinctGraph::mk_edge_table_search_key(src, atype));
    std::vector<SuccinctGraph::Assoc> result;
    std::string str;

    int32_t edge_width, dst_id_width, timestamp_width;
    int64_t cnt, curr_off;

    int32_t num_in_range = 0;

    if (t_low > t_high) {
        return result;
    }

    for (auto it = eoffs.rbegin(); it != eoffs.rend(); ++it) {
        if (num_in_range >= len) {
            break;
        }

        curr_off = *it;
        COND_LOG_E("edge table offset = %llu\n", curr_off);

        curr_off = file_log_store_.skip_until(
            curr_off, SuccinctGraph::TIMESTAMP_WIDTH_DELIM);

        file_log_store_.extract(
            str,
            curr_off,
            SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED);
        COND_LOG_E("extracted timestamp width = '%s'\n", str.c_str());
        timestamp_width = std::stoi(str);

        file_log_store_.extract(
            str,
            curr_off + SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED,
            SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED);
        COND_LOG_E("extracted dst id width = '%s'\n", str.c_str());
        dst_id_width = std::stoi(str);

        curr_off = file_log_store_.extract_until(
            str,
            curr_off +
                SuccinctGraphSerde::WIDTH_TIMESTAMP_WIDTH_PADDED +
                SuccinctGraphSerde::WIDTH_DST_ID_WIDTH_PADDED,
            SuccinctGraph::EDGE_WIDTH_DELIM);
        COND_LOG_E("extracted cnt = '%s'\n", str.c_str());
        cnt = std::stoll(str);
        assert(cnt == 1);

        curr_off = file_log_store_.extract_until(
            str, curr_off, SuccinctGraph::METADATA_DELIM);
        edge_width = std::stoi(str);
        COND_LOG_E("extracted edge width = '%s'\n", str.c_str());

        file_log_store_.extract(str, curr_off, timestamp_width);
        COND_LOG_E("extracted timestamps = '%s'\n", str.c_str());
        int64_t decoded_timestamp = SuccinctGraphSerde::decode_timestamp(str);

        if (decoded_timestamp < t_low) {
            break;
        }
        if (decoded_timestamp > t_high) {
            continue;
        }

        curr_off += timestamp_width;
        file_log_store_.extract(str, curr_off, dst_id_width);
        COND_LOG_E("extracted dst ids: '%s'\n", str.c_str());
        int64_t decoded_dst_id = SuccinctGraphSerde::decode_node_id(str);

        curr_off += dst_id_width;
        result.emplace_back();
        result.back().src_id = src;
        result.back().dst_id = decoded_dst_id;
        result.back().atype = atype;
        result.back().time = decoded_timestamp;
        file_log_store_.extract(result.back().attr, curr_off, edge_width);

        ++num_in_range;
    }
    return result;
}
