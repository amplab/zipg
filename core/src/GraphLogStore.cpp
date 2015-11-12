#include "GraphLogStore.h"

#include "GraphFormatter.hpp"

void GraphLogStore::init(int option) {
    node_table_ = std::make_shared<KVLogStore>(node_file_);
    node_table_->init(option);
}

// Serialize into the "[lengths] [attrs]" format, and call append().
void GraphLogStore::append_node(
    int64_t node_id, std::vector<std::string>& attrs)
{
    std::string delimed(GraphFormatter::format_node_attrs_str({ attrs }));
    std::string val(GraphFormatter::attach_attr_lengths(delimed));
    node_table_->append(node_id, val);
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

void get_nodes(
    std::set<int64_t>& result,
    int attr,
    const std::string& search_key)
{
    // TODO
    result.clear();
}
