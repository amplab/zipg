#include "GraphFormatter.hpp"
#include "GraphLogStore.h"
#include "GraphSuffixStore.h"
#include "KVLogStore.h"
#include "KVSuffixStore.h"
//#include "StructuredEdgeTable.h"
#include "utils.h"

#include <set>
#include <string>

void assert_eq(
    const std::vector<SuccinctGraph::Assoc>& actual,
    std::initializer_list<SuccinctGraph::Assoc> expected) {

    assert(expected.size() == actual.size());
    int i = 0;
    for (auto expected_assoc : expected) {
        auto actual_assoc = actual[i];
        assert(expected_assoc.src_id == actual_assoc.src_id);
        assert(expected_assoc.dst_id == actual_assoc.dst_id);
        assert(expected_assoc.atype == actual_assoc.atype);
        assert(expected_assoc.time == actual_assoc.time);
        assert(expected_assoc.attr == actual_assoc.attr);
        ++i;
    }
}

void assert_eq(
    const std::vector<int64_t>& actual,
    std::initializer_list<int64_t> expected)
{
    assert(expected.size() == actual.size());
    int i = 0;
    for (auto expected_elem : expected) {
        assert(actual[i] == expected_elem);
        ++i;
    }
}

void assert_eq(
    const std::set<int64_t>& actual,
    std::initializer_list<int64_t> expected)
{
    assert(expected.size() == actual.size());
    std::vector<int64_t> vec;
    for (auto it = actual.begin(); it != actual.end(); ++it)
        vec.push_back(*it);
    assert_eq(vec, expected);
}

void test_kv_log_store() {
    std::string ret;
    std::set<int64_t> keys;

    KVLogStore kv_log_store("tests/vals", "tests/ptrs");
    kv_log_store.init();

    kv_log_store.get_value(ret, 0);
    assert(ret == "1618");

    kv_log_store.search(keys, "1618");
    assert_eq(keys, { 0 });
    kv_log_store.search(keys, "kkk");
    assert_eq(keys, { });

    kv_log_store.append(2, "sup");
    kv_log_store.get_value(ret, 2);
    assert(ret == "sup");

    kv_log_store.append(4, "1618");
    kv_log_store.search(keys, "1618");
    assert_eq(keys, { 0, 4 });

    kv_log_store.append(10, "1619");
    kv_log_store.search(keys, "1619");
    assert_eq(keys, { 10 });
}

void test_kv_suffix_store() {
    KVSuffixStore kv_suffix_store("tests/vals", "tests/ptrs");
    kv_suffix_store.init();

    int key = 0;
    std::string ret;
    std::set<int64_t> keys;

    kv_suffix_store.get_value(ret, key);
    assert(ret == "1618");

    kv_suffix_store.search(keys, "1618");
    assert_eq(keys, { 0 });

    kv_suffix_store.search(keys, "16181");
    assert_eq(keys, { });

    // TODO: bugs in semantics (partial match), I think.
    kv_suffix_store.search(keys, "Mar");
    assert_eq(keys, { 1 });

    // This is correct
    kv_suffix_store.search(keys, "Martin");
    assert_eq(keys, { 1 });
}

void test_graph_log_store() {
    std::set<int64_t> keys;
    std::string attr;

    GraphLogStore graph_log_store("tests/empty", "");
    graph_log_store.init();

    std::vector<std::string> attrs{ "what's up?" };
    std::vector<std::string> attrs2{ "what's up?", "bro" };
    std::vector<std::string> attrs3{ "heyy", "", "k" };
    std::vector<std::string> attrs4{ "heyy", "k" };
    graph_log_store.append_node(0, attrs);
    graph_log_store.append_node(2, attrs2);
    graph_log_store.append_node(3, attrs3);
    graph_log_store.append_node(5, attrs4);

    // get_attr

    graph_log_store.get_attribute(attr, 0, 0);
    assert(attr == "what's up?");
    graph_log_store.get_attribute(attr, 0, 1);
    assert(attr == "");
    graph_log_store.get_attribute(attr, 2, 1);
    assert(attr == "bro");
    graph_log_store.get_attribute(attr, 2, 0);
    assert(attr == "what's up?");

    // search

    graph_log_store.get_nodes(keys, 1, "bro");
    assert_eq(keys, { 2 });

    graph_log_store.get_nodes(keys, 0, "heyy");
    assert_eq(keys, { 3, 5 });

    graph_log_store.get_nodes(keys, 0, "what's up?");
    assert_eq(keys, { 0, 2 });

    graph_log_store.get_nodes(keys, 0, "what's up?", 1, "bro");
    assert_eq(keys, { 2 });

    graph_log_store.get_nodes(keys, 0, "heyy", 2, "k");
    assert_eq(keys, { 3 });

}

void test_graph_suffix_store() {
    std::string node_file_content = GraphFormatter::to_node_table_format(
        { { "Winter", "is", "coming" },
          { "is", "Winter", "here" },
          { "George", "R", "R", "Martin", "writes", "too", "damn", "slow" }
        });
    std::string tmp_pathname(
        GraphFormatter::write_to_temp_file(node_file_content));
    LOG_E("File: %s\n", tmp_pathname.c_str());

    GraphSuffixStore graph_suffix_store(tmp_pathname, "");
    graph_suffix_store.init();
    std::set<int64_t> keys;
    std::string res;

    graph_suffix_store.get_attribute(res, 0, 1);
    assert(res == "is");

    graph_suffix_store.get_nodes(keys, 1, "is");
    assert_eq(keys, { 0 });

    graph_suffix_store.get_nodes(keys, 0, "is");
    assert_eq(keys, { 1 });

    graph_suffix_store.get_nodes(keys, 0, "Winter", 2, "coming");
    assert_eq(keys, { 0 });

    graph_suffix_store.get_nodes(keys, 0, "Winter", 3, "");
    assert_eq(keys, { 0 });

    graph_suffix_store.get_nodes(keys, 2, "R", 5, "too");
    assert_eq(keys, { 2 });

    graph_suffix_store.get_nodes(keys, 2, "R", 6, "too");
    assert_eq(keys, { });

    std::remove(tmp_pathname.c_str());
}

void test_structured_edge_table() {
    StructuredEdgeTable edge_table;
    std::vector<SuccinctGraph::Assoc> assocs;

    edge_table.add_assoc(0, 0, 0, 0, "");
    assocs = edge_table.assoc_range(0, 0, 0, 0);
    assert_eq(assocs, { });

    assocs = edge_table.assoc_range(0, 0, 0, 1);
    assert_eq(assocs, { { 0, 0, 0, 0, "" } });

    edge_table.add_assoc(0, 0, 1, 0, "newer");
    assocs = edge_table.assoc_range(0, 0, 0, 1);
    assert_eq(assocs, { { 0, 0, 1, 0, "newer" } });

    assocs = edge_table.assoc_range(0, 0, 1, 1);
    assert_eq(assocs, { { 0, 0, 0, 0, "" } });

    assocs = edge_table.assoc_range(0, 0, 1, 100);
    assert_eq(assocs, { { 0, 0, 0, 0, "" } });

    assocs = edge_table.assoc_range(0, 0, 0, 100);
    assert_eq(assocs, { { 0, 0, 1, 0, "newer" }, { 0, 0, 0, 0, "" } });
}

int main(int argc, char **argv) {

//    test_kv_log_store();
//    test_kv_suffix_store();
//
//    test_graph_log_store();
//    test_graph_suffix_store();

    test_structured_edge_table();

}
