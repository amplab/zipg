#include "GraphLogStore.h"
#include "KVLogStore.h"
#include "KVSuffixStore.h"
#include "utils.h"

#include <set>
#include <string>

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
    graph_log_store.append_node(0, attrs);
    graph_log_store.append_node(2, attrs2);

    graph_log_store.get_attribute(attr, 0, 0);
    assert(attr == "what's up?");
    graph_log_store.get_attribute(attr, 0, 1);
    assert(attr == "");
    graph_log_store.get_attribute(attr, 2, 1);
    assert(attr == "bro");
    graph_log_store.get_attribute(attr, 2, 0);
    assert(attr == "what's up?");

//    graph_log_store.get_nodes(keys, 0, "");
//    graph_log_store.get_nodes(keys, 0, "what's up?");
//    assert_eq(keys, { 0, 2 });

    LOG_E("Done: test_graph_log_store\n");
}

int main(int argc, char **argv) {

    test_kv_log_store();
    test_kv_suffix_store();

    test_graph_log_store();

}
