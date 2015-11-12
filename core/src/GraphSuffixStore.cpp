#include "GraphSuffixStore.h"

#include "GraphFormatter.hpp"

void GraphSuffixStore::init(int option) {
    node_table_ = std::make_shared<KVSuffixStore>(node_file_);
    node_table_->init(option);
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
