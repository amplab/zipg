#include "FileSuffixStore.h"
#include "GraphFormatter.hpp"
#include "GraphLogStore.h"
#include "GraphSuffixStore.h"
#include "KVLogStore.h"
#include "KVSuffixStore.h"
#include "SuccinctGraph.hpp"
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
    kv_log_store.construct();

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
    kv_suffix_store.construct();

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
    graph_log_store.construct();

    // append nodes

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

    // append edges

    graph_log_store.append_edge(0, 0, 2, 1618, "I am Edge");
    assert_eq(graph_log_store.assoc_range(0, 0, 0, 1),
        { { 0, 0, 2, 1618, "I am Edge" } });

    graph_log_store.append_edge(0, 0, 2, 1619, "I am Edge2");
    assert_eq(graph_log_store.assoc_range(0, 0, 0, 1),
        { { 0, 0, 2, 1619, "I am Edge2" } });
}

void test_graph_suffix_store() {
    std::string node_file_content = GraphFormatter::to_node_table_format(
        { { "Winter", "is", "coming" },
          { "is", "Winter", "here" },
          { "George", "R", "R", "Martin", "writes", "too", "damn", "slow" }
        });
    std::string tmp_pathname(
        GraphFormatter::write_to_temp_file(node_file_content));
    LOG_E("Node File: %s\n", tmp_pathname.c_str());

    std::string edge_file_content = "0 1 2 41842148 a b\n"
                                    "0 1618 2 93244 sup\n"
                                    "0 1 2 9324 suc\n"
                                    "0 2 0 9324 succinct is cool\n"
                                    "6 1 1 111111 abcd\n";
    std::string edge_file(
        GraphFormatter::write_to_temp_file(edge_file_content));
    std::string edge_table_file(
        GraphFormatter::write_to_temp_file(""));
    SuccinctGraph::output_edge_table(edge_file, edge_table_file);

    GraphSuffixStore graph_suffix_store(tmp_pathname, edge_table_file);
    graph_suffix_store.construct();
    std::set<int64_t> keys;
    std::string res;

    // Node Table

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

    // Edge Table

    assert_eq(graph_suffix_store.assoc_range(0, 0, 100, 1), { });

    assert_eq(graph_suffix_store.assoc_range(0, 0, 0, 1),
        { {0, 2, 0, 9324, "succinct is cool"} });

    assert_eq(graph_suffix_store.assoc_range(0, 2, 0, 2),
        { {0, 1, 2, 41842148, "a b"},
          {0, 1618, 2, 93244, "sup"} });

    assert_eq(graph_suffix_store.assoc_range(0, 2, 2, 1),
        { {0, 1, 2, 9324, "suc"} });

    assert_eq(graph_suffix_store.assoc_range(6, 1, 0, 1),
        { {6, 1, 1, 111111, "abcd"} });

    std::remove(tmp_pathname.c_str());
    std::remove(edge_file.c_str());
    std::remove(edge_table_file.c_str());
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

void test_file_suffix_store() {
    std::string edge_file_content = "0 1 2 41842148 a b\n"
                                    "0 1618 2 93244 sup\n"
                                    "0 1 2 9324 suc\n"
                                    "0 2 0 9324 succinct is cool\n"
                                    "6 1 1 111111 abcd\n";

    std::string edge_file(
        GraphFormatter::write_to_temp_file(edge_file_content));
    std::string edge_table_file(
        GraphFormatter::write_to_temp_file(""));
    SuccinctGraph::output_edge_table(edge_file, edge_table_file);

    LOG_E("File: %s\n", edge_table_file.c_str());

    FileSuffixStore file_suffix_store(edge_table_file);
    std::vector<int64_t> keys;
    std::string str;

    file_suffix_store.construct();

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(0, 2));
    assert(keys.size() == 1);

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(0, 1));
    assert(keys.size() == 0);

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(0, 0));
    assert(keys.size() == 1);

    file_suffix_store.extract(str, keys[0] + 1, 1);
    assert(str == "0");

    file_suffix_store.extract(str, keys[0] + 3, 1);
    assert(str == "0");

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(6, 1));
    file_suffix_store.extract(str, keys[0] + 1, 1);
    assert(str == "6");

    std::remove(edge_file.c_str());
    std::remove(edge_table_file.c_str());
}

void test_file_suffix_store2() {
    std::string edge_file_content = "0 1 2 41842148 a b\n"
                                    "0 1618 2 93244 sup\n"
                                    "0 1 2 9324 suc\n"
                                    "0 2 0 9324 succinct is cool\n"
                                    "6 1 1 111111 abcd\n";

    std::string edge_file(
        GraphFormatter::write_to_temp_file(edge_file_content));
    std::string edge_table_file(
        GraphFormatter::write_to_temp_file(""));
    SuccinctGraph::output_edge_table(edge_file, edge_table_file);

    LOG_E("File: %s\n", edge_table_file.c_str());

    FileSuffixStore fss(edge_table_file);
    std::vector<int64_t> keys;
    std::string str;
    fss.construct();

    // Now use another suffix store
    FileSuffixStore file_suffix_store(edge_table_file);
    file_suffix_store.load();

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(0, 2));
    assert(keys.size() == 1);

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(0, 1));
    assert(keys.size() == 0);

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(0, 0));
    assert(keys.size() == 1);

    file_suffix_store.extract(str, keys[0] + 1, 1);
    assert(str == "0");

    file_suffix_store.extract(str, keys[0] + 3, 1);
    assert(str == "0");

    file_suffix_store.search(
        keys, SuccinctGraph::mk_edge_table_search_key(6, 1));
    file_suffix_store.extract(str, keys[0] + 1, 1);
    assert(str == "6");

    std::remove(edge_file.c_str());
    std::remove(edge_table_file.c_str());
}


int main(int argc, char **argv) {

    test_kv_log_store();
    test_kv_suffix_store();

    test_graph_log_store();
    // TODO: incorporate file suffix store
    test_graph_suffix_store();

    test_structured_edge_table();
    test_file_suffix_store();

    test_file_suffix_store2();

}
