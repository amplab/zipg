#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/SuccinctGraphSerde.hpp"

#include <limits>
#include <sstream>
#include <thread>

#define WIDTH_TIMESTAMP SuccinctGraphSerde::WIDTH_TIMESTAMP
#define WIDTH_NODE_ID SuccinctGraphSerde::WIDTH_NODE_ID

// Hacky: represents not-specified query arguments
#define NONE -1

// Assumes all variables defined; curr_off needs to be at start of an assoc list
#define UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF() \
        curr_off += 1; \
        this->edge_table->extract(src_id_str, curr_off, SuccinctGraphSerde::WIDTH_NODE_ID_PADDED); \
        curr_off += SuccinctGraphSerde::WIDTH_NODE_ID_PADDED + 1; \
        this->edge_table->extract(atype_str, curr_off, SuccinctGraphSerde::WIDTH_ATYPE_PADDED); \
        src = std::stoll(src_id_str); \
        atype = std::stoll(atype_str); \
        curr_off += SuccinctGraphSerde::WIDTH_ATYPE_PADDED;

#ifdef LOG_DEBUG
    #define LOG(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
    #define LOG(fmt, ...)
#endif

// Used in places where we don't need the src id and atype.
inline int64_t skip_init_node_atype(int64_t curr_off) {
    return curr_off +
        1 + // node delim
        SuccinctGraphSerde::WIDTH_NODE_ID_PADDED + // padded node id
        1 + // atype delim
        SuccinctGraphSerde::WIDTH_ATYPE_PADDED; // padded atype
}

// TODO: lots of code duplication among the functions

// FIXME
const char NODE_ID_DELIM = '\x02';
const char ATYPE_DELIM = '\x03';
//const char NODE_ID_DELIM = 'A';
//const char ATYPE_DELIM = 'B';
const std::string SuccinctGraph::DELIMITERS = "<>()#$%&*+[]{}^-~;? \"',./:=@|\\_~\x02\x03\x04\x05\x06\x07\x08\x09";

SuccinctGraph::SuccinctGraph(
    std::string succinct_dir,
    bool construct,
    uint32_t sa_sampling_rate,
    uint32_t isa_sampling_rate,
    uint32_t npa_sampling_rate) {
}

SuccinctGraph::SuccinctGraph(
    std::string node_succinct_dir,
    std::string edge_succinct_dir) {

    this->node_table = new SuccinctShard(
        0,
        node_succinct_dir,
        SuccinctMode::LOAD_MEMORY_MAPPED);

    this->edge_table = new SuccinctFile(
        edge_succinct_dir,
        SuccinctMode::LOAD_MEMORY_MAPPED);
}

SuccinctGraph& SuccinctGraph::set_npa_sampling_rate(uint32_t sampling_rate) {
    this->npa_sampling_rate = sampling_rate;
    return *this;
}

SuccinctGraph& SuccinctGraph::set_sa_sampling_rate(uint32_t sampling_rate) {
    this->sa_sampling_rate = sampling_rate;
    return *this;
}

SuccinctGraph& SuccinctGraph::set_isa_sampling_rate(uint32_t sampling_rate) {
    this->isa_sampling_rate = sampling_rate;
    return *this;
}

void SuccinctGraph::construct_node_table(const std::string& node_file) {
    printf("Constructing node table with npa %d, sa %d, isa %d\n",
        npa_sampling_rate, sa_sampling_rate, isa_sampling_rate);
    this->node_table = new SuccinctShard(
        0,
        node_file,
        SuccinctMode::CONSTRUCT_IN_MEMORY,
        sa_sampling_rate,
        isa_sampling_rate,
        npa_sampling_rate
    );
    this->node_table->serialize();
}

SuccinctGraph& SuccinctGraph::construct(
    std::string node_file,
    std::string edge_file) {

    // construct node table in parallel
    std::thread node_table_thread(
        &SuccinctGraph::construct_node_table, this, node_file);

    fprintf(stderr, "Initializing edge table (SuccinctFile)\n");
    std::map<AssocListKey, std::vector<Assoc>> assoc_map;
    std::string line, token;
    std::ifstream edge_file_stream(edge_file);
    AType atype;
    Timestamp time;
    NodeId src_id, dst_id;

    while (std::getline(edge_file_stream, line)) {
        std::stringstream ss(line);
        int token_idx = 0;
        while (std::getline(ss, token, ' ')) {
            ++token_idx;
            if (token_idx == 1) src_id = std::stol(token);
            else if (token_idx == 2) dst_id = std::stol(token);
            else if (token_idx == 3) atype = std::stol(token);
            else if (token_idx == 4) time = std::stol(token);
            token.clear();
            if (token_idx == 4) break;
        }
        std::getline(ss, token); // rest of the data is attr
        Assoc assoc = { src_id, dst_id, atype, time, token };
        assoc_map[std::make_pair(src_id, atype)].push_back(assoc);
    }
    edge_file_stream.close();

    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        std::sort(it->second.begin(),
                  it->second.end(),
                  cmp_assoc_by_decreasing_time);
    }

    // Serialize to an .edge_table file (flat file layout)
    size_t postfix_pos = edge_file.rfind(".assoc");
    std::string edge_file_name = edge_file + ".edge_table";
    if (postfix_pos != std::string::npos)
        edge_file_name = edge_file.replace(postfix_pos, 6, ".edge_table");
    std::ofstream edge_file_out(edge_file_name);

    for (auto it = assoc_map.begin(); it != assoc_map.end(); ++it) {
        auto src_id_and_atype = it->first;

        edge_file_out << NODE_ID_DELIM
            << SuccinctGraphSerde::pad_node_id(src_id_and_atype.first);

        edge_file_out << ATYPE_DELIM
            << SuccinctGraphSerde::pad_atype(src_id_and_atype.second);

        std::vector<Assoc> assoc_list = it->second;

        int32_t edge_width = assoc_list.begin()->attr.length();
        int64_t data_width = assoc_list.size() *
            (WIDTH_TIMESTAMP +
             WIDTH_NODE_ID +
             edge_width);

        edge_file_out << SuccinctGraphSerde::pad_edge_width(edge_width)
            << SuccinctGraphSerde::pad_data_width(data_width);

        // timestamps
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            std::string encoded = SuccinctGraphSerde::encode_timestamp(
                it2->time);

            if (SuccinctGraphSerde::decode_timestamp(encoded) != it2->time) {
                printf("Failed: time = [%lld], encoded = [%s], decoded = [%lld]\n",
                    it2->time, encoded.c_str(), SuccinctGraphSerde::decode_timestamp(encoded));
                assert(0);
            }

            edge_file_out << encoded;
        }
        // dst node ids
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            std::string encoded = SuccinctGraphSerde::encode_node_id(
                it2->dst_id);

            if (SuccinctGraphSerde::decode_node_id(encoded) != it2->dst_id) {
                printf("Failed: dst_id = [%lld], encoded = [%s], decoded = [%lld]\n",
                    it2->dst_id, encoded.c_str(), SuccinctGraphSerde::decode_node_id(encoded));
                assert(0);
            }

            edge_file_out << encoded;
        }
        // edge attributes
        for (auto it2 = assoc_list.begin(); it2 != assoc_list.end(); ++it2) {
            std::string attr = it2->attr; // note: no encoding
            assert(attr.length() == edge_width);
            edge_file_out << attr;
        }
    }
    edge_file_out << "\n"; // FIXME: without this, SuccinctCore ctor segfaults
    edge_file_out.close();
    printf("Edge table written out to disk, now to Succinct-encode it\n");

    printf("constructing edge table with npa %d, sa %d, isa %d\n",
        npa_sampling_rate, sa_sampling_rate, isa_sampling_rate);
    this->edge_table = new SuccinctFile(
        edge_file_name,
        SuccinctMode::CONSTRUCT_IN_MEMORY,
        sa_sampling_rate,
        isa_sampling_rate,
        npa_sampling_rate);
    size_t num_bytes = this->edge_table->serialize();
    printf("Succinct-encoded edge table, number of bytes written: %zu\n",
        num_bytes);

    this->node_file_pathname = node_file;
    this->edge_file_pathname = edge_file;

    node_table_thread.join();
    return *this;
}

// Note: this is supposed to be used in testing only.
void SuccinctGraph::remove_generated_files() {
    char* cmd = new char[999];
    sprintf(cmd, "rm -rf %s", (this->node_file_pathname + ".succinct").c_str());
    system(cmd);

    sprintf(cmd, "rm -rf %s",
        (this->edge_file_pathname + ".edge_table").c_str());
    system(cmd);

    sprintf(cmd, "rm -rf %s",
        (this->edge_file_pathname + ".edge_table.succinct").c_str());
    system(cmd);

    delete[] cmd;
}

std::vector<int64_t>
SuccinctGraph::get_edge_table_offsets(NodeId id, AType atype) {
    std::vector<int64_t> res;
    std::string key(1, NODE_ID_DELIM);

    if (id == NONE && atype == NONE) {
        this->edge_table->search(res, key);
    } else if (atype == NONE) {
        this->edge_table->search(
            res, key + SuccinctGraphSerde::pad_node_id(id));
    } else if (id == NONE) {
        this->edge_table->search(
            res,
            std::string(1, ATYPE_DELIM) + SuccinctGraphSerde::pad_atype(atype));
        // shift to start of each assoc list
        for (int i = 0; i < res.size(); ++i) {
            res[i] -= SuccinctGraphSerde::WIDTH_NODE_ID_PADDED;
            res[i] -= 1; // delim
        }
    } else {
        key += SuccinctGraphSerde::pad_node_id(id) +
            ATYPE_DELIM +
            SuccinctGraphSerde::pad_atype(atype);
        this->edge_table->search(res, key);
        assert(res.size() <= 1);
    }
    return res;
}

void SuccinctGraph::obj_get(std::string& result, int64_t obj_id) {
    this->node_table->get(result, obj_id);
}

std::vector<SuccinctGraph::Assoc> SuccinctGraph::assoc_range(
    int64_t src, int64_t atype, int32_t off, int32_t len) {

    printf("assoc_range(src = %lld, atype = %lld, off = %d, len = %d)\n",
        src, atype, off, len);

    std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);

    std::vector<Assoc> result;
    std::string edge_width_str, data_width, timestamps, dst_ids, attrs;
    std::string src_id_str, atype_str;
    int32_t edge_width;
    int64_t cnt, curr_off;

    if (off == NONE) off = 0; // extract from head
    int32_t len_saved = len;

    for (auto it = eoffs.begin(); it != eoffs.end(); ++it) {
        curr_off = *it;
        LOG("edge table offset = %llu\n", curr_off);
        if (curr_off == -1) continue;

        // Since the passed-in src and atype can be None, extract nonetheless
        UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF();

        this->edge_table->extract(
            edge_width_str, curr_off, SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED);
        LOG("extracted edge width = '%s'\n", edge_width_str.c_str());
        edge_width = std::stoi(edge_width_str);

        curr_off += SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED;
        this->edge_table->extract(
            data_width, curr_off, SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED);
        LOG("extracted data width = '%s'\n", data_width.c_str());

        assert(std::stol(data_width) %
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width) == 0);
        cnt = std::stol(data_width) /
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);
        LOG("cnt = %llu\n", cnt);

        // if len is wildcard, extract all that's left
        len = std::min((int64_t) len_saved, cnt - off);
        if (len_saved == NONE) len = cnt - off;
        assert(off + len <= cnt);
        if (len <= 0) continue;

        curr_off += SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED;
        this->edge_table->extract(
            timestamps,
            curr_off + off * WIDTH_TIMESTAMP,
            len * WIDTH_TIMESTAMP);
        LOG("extracted timestamps = '%s'\n", timestamps.c_str());

        curr_off += cnt * WIDTH_TIMESTAMP;
        this->edge_table->extract(
            dst_ids,
            curr_off + off * WIDTH_NODE_ID,
            len * WIDTH_NODE_ID);
        LOG("extracted dst ids: '%s'\n", dst_ids.c_str());

        curr_off += cnt * WIDTH_NODE_ID;
        this->edge_table->extract(attrs,
                                  curr_off + off * edge_width,
                                  len * edge_width);
        LOG("extracted attrs = '%s'\n", attrs.c_str());

        std::vector<int64_t> decoded_timestamps =
            SuccinctGraphSerde::decode_multi_timestamps(timestamps);

        std::vector<int64_t> decoded_dst_ids =
            SuccinctGraphSerde::decode_multi_node_ids(dst_ids);

        for (int i = 0; i < decoded_timestamps.size(); ++i) {
            result.push_back(
                { src,
                  decoded_dst_ids[i],
                  atype,
                  decoded_timestamps[i],
                  attrs.substr(i * edge_width, edge_width)
                });
        }
        LOG("\n");
    }
    return result;
}

// Basic impl idea: performs binary search on the timestamps.
std::vector<SuccinctGraph::Assoc> SuccinctGraph::assoc_get(
    int64_t src,
    int64_t atype,
    std::set<int64_t> dst_id_set,
    int64_t t_low,
    int64_t t_high) {

    printf("assoc_get(src = %lld, atype = %lld, dstIdSet = ..., tLow = %lld, tHigh = %lld)\n",
        src, atype, t_low, t_high);
    std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);

    std::vector<Assoc> result;
    std::string edge_width_str, data_width, timestamps, dst_ids, attrs;
    std::string src_id_str, atype_str, timestamp;
    Timestamp ts;
    int32_t edge_width;
    int64_t cnt, curr_off;

    for (auto it = eoffs.begin(); it != eoffs.end(); ++it) {
        curr_off = *it;
        LOG("edge table offset = %llu\n", curr_off);
        if (curr_off == -1) continue;

        // Since the passed-in src and atype can be None, extract nonetheless
        UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF();

        this->edge_table->extract(
            edge_width_str, curr_off, SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED);
        LOG("extracted edge width = '%s'\n", edge_width_str.c_str());
        edge_width = std::stoi(edge_width_str);

        curr_off += SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED;
        this->edge_table->extract(
            data_width, curr_off, SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED);
        LOG("extracted data width = '%s'\n", data_width.c_str());

        curr_off += SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED;

        assert(std::stol(data_width) %
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width) == 0);
        cnt = std::stol(data_width) /
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);
        LOG("cnt = %llu\n", cnt);

        int range_left, range_right; // in-range: [left, right]

        // binary search: locates smallest t s.t. t >= t_low
        // invariant: target in [l, r)
        int l = 0, r = cnt, m;
        if (t_low != NONE) {
            // check t_l >= t_low
            this->edge_table->extract(timestamp, curr_off, WIDTH_TIMESTAMP);
            ts = SuccinctGraphSerde::decode_timestamp(timestamp);
            if (ts < t_low) continue;

            while (l + 1 < r) {
                m = (l + r) / 2;
                this->edge_table->extract(
                    timestamp, curr_off + m * WIDTH_TIMESTAMP, WIDTH_TIMESTAMP);
                ts = SuccinctGraphSerde::decode_timestamp(timestamp);
                if (ts >= t_low) l = m; // note timestamps are decreasing
                else r = m;
            }
            assert(l + 1 == r);
            range_right = l;
        } else {
            // if no time lower bound, just extract all early edges
            range_right = cnt - 1;
        }

        // binary search: locates largest t s.t. t <= t_high
        // invariant: target in (l, r]
        if (t_high != NONE) {
            l = -1;
            r = cnt - 1;

            // check t_r <= t_high
            this->edge_table->extract(timestamp, curr_off + r * WIDTH_TIMESTAMP, WIDTH_TIMESTAMP);
            ts = SuccinctGraphSerde::decode_timestamp(timestamp);
            if (ts > t_high) continue;

            while (l + 1 < r) {
                m = (l + r) / 2;
                this->edge_table->extract(
                    timestamp, curr_off + m * WIDTH_TIMESTAMP, WIDTH_TIMESTAMP);
                ts = SuccinctGraphSerde::decode_timestamp(timestamp);
                if (ts > t_high) l = m;
                else r = m;
            }
            assert(l + 1 == r);
            range_left = r;
        } else {
            // if no time lower bound, just extract all latest edges
            range_left = 0;
        }

        LOG("range left: %d, range right: %d, cnt: %lld\n", range_left, range_right, cnt);
        if (range_left > range_right) continue;

        // extract in-range timestamps
        this->edge_table->extract(
            timestamps,
            curr_off + range_left * WIDTH_TIMESTAMP,
            (range_right - range_left + 1) * WIDTH_TIMESTAMP);
        LOG("extracted timestamps = '%s'\n", timestamps.c_str());

        // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
        curr_off += cnt * WIDTH_TIMESTAMP;
        this->edge_table->extract(
            dst_ids,
            curr_off + range_left * WIDTH_NODE_ID,
            (range_right - range_left + 1) * WIDTH_NODE_ID);
        LOG("extracted dst ids: '%s'\n", dst_ids.c_str());

        std::vector<int64_t> decoded_dst_ids =
            SuccinctGraphSerde::decode_multi_node_ids(dst_ids);

        // filter
        std::vector<int64_t> in_set_indexes;
        for (int i = 0; i < decoded_dst_ids.size(); ++i) {
            LOG("decoded_dst_id[i=%d] = %lld\n", i, decoded_dst_ids[i]);

            if (dst_id_set.count(decoded_dst_ids[i]) != 0) {
                in_set_indexes.push_back(range_left + i);
                LOG("pass filter id: %d, decoded_dst_id[i] = %lld\n", range_left + i, decoded_dst_ids[i]);
            }
        }

        // TODO: another choice is to do a single extract then filter; evaluate?
        // Now extract only the in-set (and in-range) attrs
        curr_off += cnt * WIDTH_NODE_ID;
        std::vector<std::string> valid_attrs;
        int idx;
        for (int i = 0; i < in_set_indexes.size(); ++i) {
            idx = in_set_indexes[i];
            std::string attr;
            this->edge_table->extract(attr, curr_off + idx * edge_width, edge_width);
            valid_attrs.push_back(attr);
        }

        std::vector<int64_t> decoded_timestamps =
            SuccinctGraphSerde::decode_multi_timestamps(timestamps);
        for (int i = 0; i < in_set_indexes.size(); ++i) {
            idx = in_set_indexes[i];
            // decoded dst ids and timestamps start w/ absolute idx range_left
            result.push_back(
                { src, // from UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF
                  decoded_dst_ids[idx - range_left],
                  atype, // from UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF
                  decoded_timestamps[idx - range_left],
                  valid_attrs[i]
                });
        }
    }
    return result;
}

int64_t SuccinctGraph::assoc_count(int64_t src, int64_t atype) {
    std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);
    int64_t total_cnt = 0;
    std::string edge_width_str, data_width;

    for (auto curr_off : eoffs) {
        if (curr_off == -1) continue;
        curr_off = skip_init_node_atype(curr_off);

        this->edge_table->extract(
            edge_width_str, curr_off, SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED);
        int32_t edge_width = std::stoi(edge_width_str);

        curr_off += SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED;
        this->edge_table->extract(
            data_width, curr_off, SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED);

        assert(std::stol(data_width) %
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width) == 0);
        total_cnt += std::stol(data_width) /
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);
    }
    return total_cnt;
}

std::vector<SuccinctGraph::Assoc> SuccinctGraph::assoc_time_range(
    int64_t src,
    int64_t atype,
    int64_t t_low,
    int64_t t_high,
    int32_t len) {

    printf("assoc_time_range(src = %lld, atype = %lld, tLow = %lld, tHigh = %lld, len = %d)\n",
        src, atype, t_low, t_high, len);

    std::vector<int64_t> eoffs = get_edge_table_offsets(src, atype);

    std::vector<Assoc> result;
    std::string edge_width_str, data_width, timestamps, dst_ids, attrs;
    std::string src_id_str, atype_str, timestamp;
    Timestamp ts;
    int32_t edge_width;
    int64_t cnt;

    int32_t len_saved = len;

    for (int64_t curr_off : eoffs) {
        LOG("edge table offset = %llu\n", curr_off);
        if (curr_off == -1) continue;

        // Since the passed-in src and atype can be None, extract nonetheless
        UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF();

        this->edge_table->extract(
            edge_width_str, curr_off, SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED);
        LOG("extracted edge width = '%s'\n", edge_width_str.c_str());
        edge_width = std::stoi(edge_width_str);

        curr_off += SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED;
        this->edge_table->extract(
            data_width, curr_off, SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED);
        LOG("extracted data width = '%s'\n", data_width.c_str());

        curr_off += SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED;

        assert(std::stol(data_width) %
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width) == 0);
        cnt = std::stol(data_width) /
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);
        LOG("cnt = %llu\n", cnt);

        // if len is wildcard, extract all that's left
        if (len_saved == NONE) len = cnt;

        int range_left, range_right; // in-range: [left, right]
        int l, r, m;

        if (t_low != NONE) {
            l = 0;
            r = cnt;

            // check t_l >= t_low
            this->edge_table->extract(timestamp, curr_off, WIDTH_TIMESTAMP);
            ts = SuccinctGraphSerde::decode_timestamp(timestamp);
            if (ts < t_low) continue;

            // binary search: locates smallest t s.t. t >= t_low
            // invariant: target in [l, r)
            while (l + 1 < r) {
                m = (l + r) / 2;
                this->edge_table->extract(
                    timestamp, curr_off + m * WIDTH_TIMESTAMP, WIDTH_TIMESTAMP);
                ts = SuccinctGraphSerde::decode_timestamp(timestamp);
                if (ts >= t_low) l = m; // note timestamps are decreasing
                else r = m;
            }
            assert(l + 1 == r);
            range_right = l;
        } else {
            // if no time lower bound, just extract all early edges
            range_right = cnt - 1;
        }

        // binary search: locates largest t s.t. t <= t_high
        // invariant: target in (l, r]
        if (t_high != NONE) {
            l = -1;
            r = cnt - 1;

            // check t_r <= t_high
            this->edge_table->extract(timestamp, curr_off + r * WIDTH_TIMESTAMP, WIDTH_TIMESTAMP);
            ts = SuccinctGraphSerde::decode_timestamp(timestamp);
            if (ts > t_high) continue;

            while (l + 1 < r) {
                m = (l + r) / 2;
                this->edge_table->extract(
                    timestamp, curr_off + m * WIDTH_TIMESTAMP, WIDTH_TIMESTAMP);
                ts = SuccinctGraphSerde::decode_timestamp(timestamp);
                if (ts > t_high) l = m;
                else r = m;
            }
            assert(l + 1 == r);
            range_left = r;
        } else {
            // if no time lower bound, just extract all latest edges
            range_left = 0;
        }

        // limit to first `len` edges
        range_right = std::min(range_right, range_left + len - 1);
        LOG("range left: %d, range right: %d, cnt: %lld\n", range_left, range_right, cnt);
        if (range_left > range_right) continue;

        // extract in-range timestamps
        std::string timestamps;
        this->edge_table->extract(timestamps,
                                  curr_off + range_left * WIDTH_TIMESTAMP,
                                  (range_right - range_left + 1) * WIDTH_TIMESTAMP);
        LOG("extracted timestamps = '%s'\n", timestamps.c_str());

        // extract in-range dst ids: i.e. whose idx in [range_left, range_right]
        curr_off += cnt * WIDTH_TIMESTAMP;
        std::string dst_ids;
        this->edge_table->extract(dst_ids,
                                  curr_off + range_left * WIDTH_NODE_ID,
                                  (range_right - range_left + 1) * WIDTH_NODE_ID);
        LOG("extracted dst ids: '%s'\n", dst_ids.c_str());

        curr_off += cnt * WIDTH_NODE_ID;
        std::string attrs;
        this->edge_table->extract(attrs,
                                  curr_off + range_left * edge_width,
                                  (range_right - range_left + 1) * edge_width);
        LOG("extracted attrs = '%s'\n", attrs.c_str());

        std::vector<int64_t> decoded_timestamps =
            SuccinctGraphSerde::decode_multi_timestamps(timestamps);
        std::vector<int64_t> decoded_dst_ids =
            SuccinctGraphSerde::decode_multi_node_ids(dst_ids);
        for (int i = 0; i < decoded_timestamps.size(); ++i) {
            result.push_back(
                { src, // from UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF
                  decoded_dst_ids[i],
                  atype, // from UNSAFE_EXTRACT_SRC_ATYPE_AND_INC_OFF
                  decoded_timestamps[i],
                  attrs.substr(i * edge_width, edge_width)
                });
        }
    }
    return result;
}

std::string SuccinctGraph::succinct_directory() {
    return this->succinct_dir;
}

int64_t SuccinctGraph::num_nodes() {
    return this->node_table->num_keys() - 1; // FIXME: a bug in SuccinctShard
}

int64_t SuccinctGraph::num_edges() {
    return edges;
}

int64_t SuccinctGraph::num_attributes() {
    return NODE_NUM_ATTRS;
}

size_t SuccinctGraph::storage_size() {
}

size_t SuccinctGraph::serialize() {
}

/******* Primitive APIs *******/

// Assumes the values in node table have fixed number of attributes, each of
// which has fixed width (NODE_ATTR_SIZE), and that each attr is prefixed by a
// delimiter (in fact unique, but that fact is irrelevant here).
void SuccinctGraph::get_attribute(
    std::string& result,
    int64_t node_id,
    int attr) {

    assert(attr < NODE_NUM_ATTRS);
    this->node_table->access(
        result, node_id, attr * (NODE_ATTR_SIZE + 1) + 1, NODE_ATTR_SIZE);
}

void SuccinctGraph::get_neighbors(std::vector<int64_t>& result, int64_t node) {
    std::vector<int64_t> decoded, offsets;
    this->edge_table->search(
        offsets, NODE_ID_DELIM + SuccinctGraphSerde::pad_node_id(node));

    std::string edge_width_str, data_width, dst_ids;
    result.clear();
    for (auto it = offsets.begin(); it != offsets.end(); ++it) {
        int64_t curr_off = *it;
        curr_off = skip_init_node_atype(curr_off);

        this->edge_table->extract(
            edge_width_str, curr_off, SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED);
        int32_t edge_width = std::stoi(edge_width_str);

        curr_off += SuccinctGraphSerde::WIDTH_EDGE_WIDTH_PADDED;
        this->edge_table->extract(
            data_width, curr_off, SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED);

        curr_off += SuccinctGraphSerde::WIDTH_DATA_WIDTH_PADDED;

        assert(std::stol(data_width) %
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width) == 0);

        int64_t cnt = std::stol(data_width) /
            (WIDTH_TIMESTAMP + WIDTH_NODE_ID + edge_width);

        curr_off += cnt * WIDTH_TIMESTAMP;

        this->edge_table->extract(dst_ids, curr_off, cnt * WIDTH_NODE_ID);

        std::vector<int64_t> decoded =
            SuccinctGraphSerde::decode_multi_node_ids(dst_ids);

        result.insert(result.end(), decoded.begin(), decoded.end());
    }
}

// Scans neighbors and looks for those that contain the desired attr.
void SuccinctGraph::get_neighbors(
    std::vector<int64_t>& result,
    int64_t node_id,
    int attr,
    const std::string& search_key) {

    result.clear();
    size_t pos;
    std::string attributes;
    std::vector<int64_t> nbhrs;
    get_neighbors(nbhrs, node_id);

    for (auto it = nbhrs.begin(); it != nbhrs.end(); ++it) {
        this->node_table->get(attributes, *it);
        pos = attributes.find(SuccinctGraph::DELIMITERS[attr] + search_key);
        LOG("nbhr id %lld, search key '%s', pos = %d\n",
            *it, (SuccinctGraph::DELIMITERS[attr] + search_key).c_str(), pos);
        if (pos != std::string::npos) result.push_back(*it);
    }
}

void SuccinctGraph::get_nodes(
    std::set<int64_t>& result,
    int attr,
    const std::string& search_key) {

    result.clear();
    this->node_table->search(result, DELIMITERS[attr] + search_key);
}

void SuccinctGraph::get_nodes(
    std::set<int64_t>& result,
    int attr1,
    const std::string& search_key1,
    int attr2,
    const std::string& search_key2) {

    result.clear();
    std::set<int64_t> s1, s2;
    this->node_table->search(s1, DELIMITERS[attr1] + search_key1);
    this->node_table->search(s2, DELIMITERS[attr2] + search_key2);
    // result.end() is a hint that supposedly is faster than .begin()
    std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                          std::inserter(result, result.end()));
}
