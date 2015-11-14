#include "GraphFormatter.hpp"

#include "utils.h"

#include <set>
#include <string>

int main(int argc, char **argv) {

    std::string assoc_list_shard(argv[1]);
    std::set<std::pair<int64_t, int64_t>> set, new_set;
    GraphFormatter::read_assoc_list(assoc_list_shard, set);
    LOG_E("%d assoc lists in shard '%s'\n",
        set.size(), assoc_list_shard.c_str());

    constexpr size_t num_nodes = 41652230;
    constexpr size_t num_edges = 1468365182;
    constexpr size_t num_atypes = 5;
    constexpr size_t edges_per_suff_store = 3.5 * 1000000; // 512 MB

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_atype(0, num_atypes - 1);

    size_t num_assoc_list_with_updates = 0;

    for (size_t i = 0; i < edges_per_suff_store; ++i) {
        int64_t src = uni_node(rng);
        int64_t atype = uni_atype(rng);
        auto pair = std::make_pair(src, atype);

        // NOTE: only add edges to existing assoc lists
        while (set.count(pair) == 0) {
            src = uni_node(rng);
            atype = uni_atype(rng);
            pair = std::make_pair(src, atype);
        }

        if (new_set.count(pair) == 0) {
            ++num_assoc_list_with_updates;
        }
        new_set.insert(pair);
    }
    LOG_E("Number of assoc lists that have updates now: %lld\n",
        num_assoc_list_with_updates);
    LOG_E("Number of assoc lists in the new shard (filled by updates): %lld\n",
        new_set.size());

}
