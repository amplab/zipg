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

void assert_eq(
    const std::vector<std::string>& actual,
    std::initializer_list<std::string> expected)
{
    assert(expected.size() == actual.size());
    int i = 0;
    for (const auto& expected_elem : expected) {
        assert(actual[i] == expected_elem);
        ++i;
    }
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

    kv_log_store.append("sup");
    kv_log_store.get_value(ret, 2);
    assert(ret == "sup");

    kv_log_store.append("1618");
    kv_log_store.search(keys, "1618");
    assert_eq(keys, { 0, 3 });

    kv_log_store.append("1619");
    kv_log_store.search(keys, "1619");
    assert_eq(keys, { 4 });
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
    graph_log_store.append_node(attrs);
    graph_log_store.append_node(attrs2);
    graph_log_store.append_node(attrs3);
    graph_log_store.append_node(attrs4);

    // get_attr

    graph_log_store.get_attribute(attr, 0, 0);
    assert(attr == "what's up?");
    graph_log_store.get_attribute(attr, 0, 1);
    assert(attr == "");
    graph_log_store.get_attribute(attr, 1, 1);
    assert(attr == "bro");
    graph_log_store.get_attribute(attr, 1, 0);
    assert(attr == "what's up?");

    // search

    graph_log_store.get_nodes(keys, 1, "bro");
    assert_eq(keys, { 1 });

    graph_log_store.get_nodes(keys, 0, "heyy");
    assert_eq(keys, { 2, 3 });

    graph_log_store.get_nodes(keys, 0, "what's up?");
    assert_eq(keys, { 0, 1 });

    graph_log_store.get_nodes(keys, 0, "what's up?", 1, "bro");
    assert_eq(keys, { 1 });

    graph_log_store.get_nodes(keys, 0, "heyy", 2, "k");
    assert_eq(keys, { 2 });

    // append edges

    graph_log_store.append_edge(0, 2, 0, 1618, "I am Edge");
    assert_eq(graph_log_store.assoc_range(0, 0, 0, 1),
        { { 0, 2, 0, 1618, "I am Edge" } });

    graph_log_store.append_edge(0, 2, 0, 1619, "I am Edge2");
    assert_eq(graph_log_store.assoc_range(0, 0, 0, 1),
        { { 0, 2, 0, 1619, "I am Edge2" } });
}

void test_graph_log_store2() {
    std::string node_file_content = GraphFormatter::to_node_table_format(
        { { "Winter", "is", "coming" },
          { "is", "Winter", "here" },
          { "George", "R", "R", "Martin", "writes", "too", "damn", "slow" }
        });
    std::string tmp_pathname(
        GraphFormatter::write_to_temp_file(node_file_content));

    std::string edge_file_content = "0 1 2 41842148 a b\n"
                                    "0 1618 2 93244 sup\n"
                                    "0 1 2 9324 suc\n"
                                    "0 2 0 9324 succinct is cool\n"
                                    "6 1 1 111111 abcd\n";
    std::string edge_file(
        GraphFormatter::write_to_temp_file(edge_file_content));
    COND_LOG_E("Edge table wrote out to '%s'\n", edge_file.c_str());

    GraphLogStore graph_log_store(tmp_pathname, edge_file);
    graph_log_store.construct();

    // TAO

    assert_eq(graph_log_store.assoc_range(0, 0, 100, 1), { });

    assert_eq(graph_log_store.assoc_range(0, 0, 0, 1),
        { {0, 2, 0, 9324, "succinct is cool"} });

    SuccinctGraph::print_assoc_results(graph_log_store.assoc_range(0, 2, 0, 2));
    assert_eq(graph_log_store.assoc_range(0, 2, 0, 2),
        { {0, 1, 2, 41842148, "a b"},
          {0, 1618, 2, 93244, "sup"} });

    assert_eq(graph_log_store.assoc_range(0, 2, 2, 1),
        { {0, 1, 2, 9324, "suc"} });

    assert_eq(graph_log_store.assoc_range(6, 1, 0, 1),
        { {6, 1, 1, 111111, "abcd"} });


    assert(graph_log_store.assoc_count(0, 0) == 1);
    assert(graph_log_store.assoc_count(0, 2) == 3);
    assert(graph_log_store.assoc_count(6, 1) == 1);
    assert(graph_log_store.assoc_count(5, 1) == 0);

    // assoc_get() tests

    std::set<int64_t> dst_id_set;
    dst_id_set.insert(1618);

    assert_eq(graph_log_store.assoc_get(
        0, 2, dst_id_set, 1435055631064LL,1436667356522LL),
        {});
    assert_eq(graph_log_store.assoc_get(
        0, 2, dst_id_set, 1435055631064,1436667356522),
        {});

    assert_eq(graph_log_store.assoc_get(0, 2, dst_id_set, 9324, 93245),
        { {0, 1618, 2, 93244, "sup"} });

    dst_id_set.insert(1);
    assert_eq(graph_log_store.assoc_get(0, 2, dst_id_set, 9324, 93245),
        { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

    // assoc_time_range() tests

    assert_eq(graph_log_store.assoc_time_range(6, 1, 1, 99999999, 10),
        { {6, 1, 1, 111111, "abcd"} });

    // nothing
    assert_eq(graph_log_store.assoc_time_range(0, 0, 0, 1, 10), { });

    SuccinctGraph::print_assoc_results(
        graph_log_store.assoc_time_range(0, 2, 900, 93244, 2)); // 2 edges
    assert_eq(graph_log_store.assoc_time_range(0, 2, 900, 93244, 2),
        { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

    SuccinctGraph::print_assoc_results(
        graph_log_store.assoc_time_range(0, 2, 900, 93244, 1)); // 1 edge
    assert_eq(graph_log_store.assoc_time_range(0, 2, 900, 93244, 1),
        { {0, 1618, 2, 93244, "sup"} });

    std::remove(tmp_pathname.c_str());
    std::remove(edge_file.c_str());
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


    assert(graph_suffix_store.assoc_count(0, 0) == 1);
    assert(graph_suffix_store.assoc_count(0, 2) == 3);
    assert(graph_suffix_store.assoc_count(6, 1) == 1);
    assert(graph_suffix_store.assoc_count(5, 1) == 0);

    // assoc_get() tests

    std::set<int64_t> dst_id_set;
    dst_id_set.insert(1618);

    assert_eq(graph_suffix_store.assoc_get(
        0, 2, dst_id_set, 1435055631064LL,1436667356522LL),
        {});
    assert_eq(graph_suffix_store.assoc_get(
        0, 2, dst_id_set, 1435055631064,1436667356522),
        {});

    assert_eq(graph_suffix_store.assoc_get(0, 2, dst_id_set, 9324, 93245),
        { {0, 1618, 2, 93244, "sup"} });

    dst_id_set.insert(1);
    assert_eq(graph_suffix_store.assoc_get(0, 2, dst_id_set, 9324, 93245),
        { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

    // assoc_time_range() tests

    assert_eq(graph_suffix_store.assoc_time_range(6, 1, 1, 99999999, 10),
        { {6, 1, 1, 111111, "abcd"} });

    // nothing
    assert_eq(graph_suffix_store.assoc_time_range(0, 0, 0, 1, 10), { });

    SuccinctGraph::print_assoc_results(
        graph_suffix_store.assoc_time_range(0, 2, 900, 93244, 2)); // 2 edges
    assert_eq(graph_suffix_store.assoc_time_range(0, 2, 900, 93244, 2),
        { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

    SuccinctGraph::print_assoc_results(
        graph_suffix_store.assoc_time_range(0, 2, 900, 93244, 1)); // 1 edge
    assert_eq(graph_suffix_store.assoc_time_range(0, 2, 900, 93244, 1),
        { {0, 1618, 2, 93244, "sup"} });

//    std::vector<std::string> attributes;
//    graph_suffix_store.obj_get(attributes, 0);
//    assert_eq(attributes, { "Winter", "is", "coming" });
//    graph_suffix_store.obj_get(attributes, 2);
//    assert_eq(attributes,
//        { "George", "R", "R", "Martin", "writes", "too", "damn", "slow" });
//    graph_suffix_store.obj_get(attributes, 3);
//    assert_eq(attributes, { });
//    graph_suffix_store.obj_get(attributes, 1618);
//    assert_eq(attributes, { });

    // cleanups

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

    edge_table.add_assoc(0, 1, 0, 0, "newer");
    assocs = edge_table.assoc_range(0, 0, 0, 1);
    SuccinctGraph::print_assoc_results(assocs);
    assert_eq(assocs, { { 0, 1, 0, 0, "newer" } });

    assocs = edge_table.assoc_range(0, 0, 1, 1);
    assert_eq(assocs, { { 0, 0, 0, 0, "" } });

    assocs = edge_table.assoc_range(0, 0, 1, 100);
    assert_eq(assocs, { { 0, 0, 0, 0, "" } });

    assocs = edge_table.assoc_range(0, 0, 0, 100);
    assert_eq(assocs, { { 0, 1, 0, 0, "newer" }, { 0, 0, 0, 0, "" } });
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

    test_structured_edge_table();
    test_file_suffix_store();
    test_file_suffix_store2();

    test_graph_log_store();
    test_graph_suffix_store();
    test_graph_log_store2();

}
