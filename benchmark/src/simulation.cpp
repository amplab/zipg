#include "GraphFormatter.hpp"

#include "utils.h"

#include <string>
#include <unordered_set>

#include <boost/functional/hash.hpp>

const std::string set_out_file = "all_assoc_lists.set";

void serialize(std::unordered_set<
                       std::pair<int64_t, int64_t>,
                       boost::hash< std::pair<int, int> >
               >& set)
{
    std::ofstream ofstream(set_out_file);
    for (auto it = set.begin(); it != set.end(); ++it) {
        ofstream << it->first << " " << it->second << std::endl;
    }
}

void deserialize(std::unordered_set<
                       std::pair<int64_t, int64_t>,
                       boost::hash< std::pair<int, int> >
               >& set)
{
    std::ifstream ifstream(set_out_file);
    std::string line, token;
    int64_t src, atype;
    while (std::getline(ifstream, line)) {
        std::stringstream ss(line);
        std::getline(ss, token, ' ');
        src = std::stoll(token);
        std::getline(ss, token);
        atype = std::stoll(token);
        set.insert(std::make_pair(src, atype));
    }
}

int main(int argc, char **argv) {
    size_t last_size = 0;

    std::unordered_set<
        std::pair<int64_t, int64_t>,
        boost::hash< std::pair<int, int> >
    > set, new_set;

//    for (int i = 1; i < argc; ++i) {
//        std::string assoc_list_shard(argv[i]);
//        GraphFormatter::read_assoc_list(assoc_list_shard, set);
//        LOG_E("%d assoc lists in shard '%s'\n",
//            set.size() - last_size, assoc_list_shard.c_str());
//        last_size = set.size();
//    }
//    serialize(set);

    deserialize(set);
    LOG_E("Total # assoc lists in input shards: %lld\n", set.size());

    constexpr size_t num_nodes = 41652230;
    constexpr size_t num_edges = 1468365182;
    constexpr size_t num_atypes = 5;
    constexpr size_t edges_per_suff_store = 3.5 * 1000000 * 2; // 1 GB

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int64_t> uni_node(0, num_nodes - 1);
    std::uniform_int_distribution<int> uni_atype(0, num_atypes - 1);

    size_t num_assoc_list_with_updates = 0;

    auto report = [&](size_t num_edges) {
        LOG_E("# lists that have updates w/ @ %lld new edges: %lld (%.1f\%)\n",
            num_edges,
            num_assoc_list_with_updates,
            static_cast<double>(num_assoc_list_with_updates) / set.size() * 100);
    };

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

        if (i == edges_per_suff_store / 2 || i == edges_per_suff_store / 8) {
            report(i);
        }
    }
    report(edges_per_suff_store);

}
