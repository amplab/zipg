#include "GraphLogStore.h"

#include "GraphFormatter.hpp"

void GraphLogStore::construct() {
    node_table_ = std::make_shared<KVLogStore>(4294967296ULL);
    LOG_E("GraphLogStore: Constructing Edge table\n");
    edge_table_.construct();
    LOG_E("GraphLogStore: Edge table constructed\n");
}

void GraphLogStore::load() {
	LOG_E("Loading GraphLogStore");
    node_table_ = std::make_shared<KVLogStore>(4294967296ULL);
}

// Serialize into the "[lengths] [attrs]" format, and call append().
int64_t GraphLogStore::append_node(const std::vector<std::string>& attrs)
{
	int64_t start, end;
    start = get_timestamp();
	std::string delimed(GraphFormatter::format_node_attrs_str({ attrs }));
    std::string val(GraphFormatter::attach_attr_lengths(delimed));
    end = get_timestamp();
    COND_LOG_E("Time to format attributes: %lld\n", (end - start));

    start = get_timestamp();
    int64_t node = node_table_->append(val);
    end = get_timestamp();
    COND_LOG_E("Time to append node at GraphLogStore: %lld us\n", (end - start));
    return node;
}

int GraphLogStore::append_edge(
    int64_t src,
    int64_t dst,
    int64_t atype,
    int64_t timestamp,
    const std::string& attr)
{
    if (edge_table_.num_edges() >= max_num_edges_) {
        LOG_E("append_edge failed: Edge table log store already has %d edges\n",
            max_num_edges_);
        return -1;
    }

    edge_table_.add_assoc(src, dst, atype, timestamp, attr);
    return 0;
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
