#include "KVLogStore.h"
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

int main(int argc, char **argv) {
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

    kv_log_store.append(1, "sup");
    kv_log_store.get_value(ret, 1);
    assert(ret == "sup");

    kv_log_store.append(2, "1618");
    kv_log_store.search(keys, "1618");
    assert_eq(keys, { 0, 2 });

    kv_log_store.append(3, "1619");
    kv_log_store.search(keys, "1619");
    assert_eq(keys, { 3 });

}
