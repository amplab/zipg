#include "GraphLogStore.h"

#include "GraphFormatter.hpp"

void GraphLogStore::construct() {
    node_table_ = std::make_shared<KVLogStore>(node_file_);
    node_table_->construct();
    edge_table_.construct();
}

void GraphLogStore::load() {
    node_table_ = std::make_shared<KVLogStore>(node_file_);
    node_table_->load();
    edge_table_.load();
}

// Serialize into the "[lengths] [attrs]" format, and call append().
void GraphLogStore::append_node(
    int64_t node_id, std::vector<std::string>& attrs)
{
    std::string delimed(GraphFormatter::format_node_attrs_str({ attrs }));
    std::string val(GraphFormatter::attach_attr_lengths(delimed));
    COND_LOG_E("Appending node %lld, attrs '%s'\n", node_id, val.c_str());
    if (node_table_->append(node_id, val)) {
        LOG_E("Failed append node %lld, LogStore full?\n", node_id);
        exit(-1);
    }
}
void GraphLogStore::append_edge(
    int64_t src,
    int64_t atype,
    int64_t dst,
    int64_t timestamp,
    const std::string& attr)
{
    edge_table_.add_assoc(src, atype, dst, timestamp, attr);
}

void GraphLogStore::get_attribute(
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

void GraphLogStore::get_nodes(
    std::set<int64_t>& result,
    int attr,
    const std::string& search_key)
{
    result.clear();
    node_table_->search(result,
        std::move(SuccinctGraph::mk_node_attr_key(attr, search_key)));
}

void GraphLogStore::get_nodes(
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

    COND_LOG_E("s1 size %d, s2 size %d\n", s1.size(), s2.size());

    // result.end() is a hint that supposedly is faster than .begin()
    std::set_intersection(s1.begin(), s1.end(), s2.begin(), s2.end(),
                          std::inserter(result, result.end()));
}

void GraphLogStore::obj_get(std::vector<std::string>& result, int64_t obj_id) {
    LOG_E("obj_get(%lld)\n", obj_id);
    assert(false && "benchmark should not have routed the query here!");
//    result.clear();
//    std::string str;
//    node_table_->get_value(str, obj_id); // TODO: handle non-existent?
//
//    // get the initial dist first
//    int i = 0, dist = 0;
//    while (str[i] != SuccinctGraph::NODE_TABLE_HEADER_DELIM) {
//        dist = dist * 10 + (str[i] - '0');
//        ++i;
//    }
//    ++i;
//
//    char next_delim;
//    int last_non_empty;
//
//    dist += i + 1; // pointing to the start of attr1
//    for (int attr = 0; attr < SuccinctGraph::MAX_NUM_NODE_ATTRS; ++attr) {
//        next_delim = static_cast<char>(SuccinctGraph::DELIMITERS[attr + 1]);
//        i = dist;
//        while (str[i] != next_delim) {
//            ++i;
//        }
//        if (i > dist) {
//            last_non_empty = attr;
//        }
//        assert(i <= str.length());
//        result.emplace_back(str.substr(dist, i - dist));
//        dist = i + 1;
//    }
}
