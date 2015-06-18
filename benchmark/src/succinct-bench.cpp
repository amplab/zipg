#include <cassert>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "succinct-graph/GraphFormatter.hpp"
#include "succinct-graph/SuccinctGraph.hpp"
#include "../include/GraphBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-x warmup_n] [-y measure_n] [-w warmup_file] [-q query_file] [-a neighbor_warmup ] [-b neighbor_query] [-o output_file] [succinct_dir]\n", exec);
}

void print_vector(const std::string& msg, const std::vector<int64_t>& vec) {
    printf("%s[", msg.c_str());
    for (auto it = vec.begin(); it != vec.end(); ++it)
        printf(" %lld", *it);
    printf(" ]\n");
}

void assert_eq(
    const std::vector<SuccinctGraph::Assoc>& expected,
    std::initializer_list<SuccinctGraph::Assoc> actual) {

    int i = 0;
    for (auto actual_assoc : actual) {
        auto expected_assoc = expected[i];
        assert(expected_assoc.src_id == actual_assoc.src_id);
        assert(expected_assoc.dst_id == actual_assoc.dst_id);
        assert(expected_assoc.atype == actual_assoc.atype);
        assert(expected_assoc.time == actual_assoc.time);
        assert(expected_assoc.attr == actual_assoc.attr);
        ++i;
    }
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage(argv[0]);
        // return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    int warmup_n = 20000; int measure_n = 100000;
    std::string warmup_query_file = "warmup.txt";
    std::string measure_query_file = "query_file.txt";
    std::string warmup_neighbor_file = "warmup.txt";
    std::string measure_neighbor_file = "query_file.txt";
    std::string result_file_name = "benchmark_results.txt";
    while ((c = getopt(argc, argv, "t:x:y:z:w:q:a:b:o:")) != -1) {
        switch(c) {
        case 't':
            type = std::string(optarg);
            break;
        case 'x':
            warmup_n = std::stoi(std::string(optarg));
            break;
        case 'y':
            measure_n = std::stoi(std::string(optarg));
            break;
        case 'w':
            warmup_query_file = std::string(optarg);
            break;
        case 'q':
            measure_query_file = std::string(optarg);
            break;
        case 'a':
            warmup_neighbor_file = std::string(optarg);
            break;
        case 'b':
            measure_neighbor_file = std::string(optarg);
            break;
        case 'o':
            result_file_name = std::string(optarg);
            break;
        }
    }

    if (optind == argc) {
        print_usage(argv[0]);
        // return -1;
    }

//    printf("started\n");
//    std::string F = "/Users/zongheng/workspace/amplab/succinct-graph/data/assocs/test.edge_table";
//    std::string F2 = "/Users/zongheng/workspace/amplab/succinct-graph/data/assocs/test.assoc.edge_table";
//    SuccinctCore *f = new SuccinctCore(F.c_str());
//    SuccinctFile *f = new SuccinctFile(F2.c_str());
//    printf("done\n");
//    assert(0);

    std::string succinct_dir = std::string(argv[optind]);
    SuccinctGraph* graph = new SuccinctGraph(succinct_dir, false);

    std::ofstream result_file(result_file_name, std::ios_base::app);

    GraphBenchmark bench(graph);
    if (type == "neighbor-latency") {
        bench.benchmark_neighbor_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "neighbor-throughput") {
        std::pair<double, double> thput_pair = bench.benchmark_neighbor_throughput(
                warmup_query_file, measure_query_file);
        result_file << "Get Neighbor Throughput: " << thput_pair.first << "\n";
        result_file << "Get Edges Throughput: " << thput_pair.second << "\n";
    } else if (type == "node-latency") {
        bench.benchmark_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "node-throughput") {
        double thput = bench.benchmark_node_throughput(warmup_query_file, measure_query_file);
        result_file << "Get Name Throughput: " << thput << "\n\n";
    } else if (type == "mix-throughput") {
        double thput = bench.benchmark_mix_throughput(
                warmup_neighbor_file, measure_neighbor_file,
                warmup_query_file, measure_query_file);
        result_file << "Mix throughput: " << thput << "\n\n";
    } else if (type == "mix-latency") {
        bench.benchmark_mix_latency(result_file_name, warmup_n, measure_n,
                warmup_neighbor_file, measure_neighbor_file,
                warmup_query_file, measure_query_file);
    } else if (type == "node-node-latency") {
        bench.benchmark_node_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "neighbor-node-latency") {
        bench.benchmark_neighbor_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);
    } else if (type == "graph-construct") {
        // case: construct from node & edge file
        std::string node_file = succinct_dir; // hacky naming
        std::string assoc_file = std::string(argv[optind + 1]);

        graph = new SuccinctGraph(succinct_dir, true); // no-op
        graph->construct(node_file, assoc_file);

        printf("SuccinctGraph construction done\n");

    } else if (type == "graph-format") {
        GraphFormatter::format_node_file(succinct_dir);
        printf("Node file formatted\n");
    } else if (type == "higgs-format") {
        std::string in_file = succinct_dir;
        std::string assoc_out_file = std::string(argv[optind + 1]);

        GraphFormatter::format_higgs_activity_file(
            in_file, assoc_out_file, 128);

        // 456626 + 1, since unclear if original data is 0-indexed
        int num_nodes = 456627;
        int num_attr = 4;
        int freq = 1000;
        int len = 200; // so total node attr = 800 bytes

        printf("about to call\n");
        GraphFormatter::create_random_node_table(
            std::string(argv[optind + 2]),
            num_nodes,
            num_attr,
            freq,
            len
        );

    } else if (type == "graph-test") {
        // case: load (mmap) constructed files
        // TODO: write as an automatic test suite (e.g. write out tmp file)
        std::string node_succinct_dir = succinct_dir;
        std::string edge_succinct_dir = std::string(argv[optind + 1]);
        graph = new SuccinctGraph(succinct_dir, true); // no-op
        graph->load(node_succinct_dir, edge_succinct_dir);
        printf("Loaded SuccinctGraph from files.\n");

        // assoc_range() tests

        // all edges from node 0
        assert_eq(graph->assoc_range(0, -1, -1, -1),
            { {0, 2, 0, 9324, "succinct is cool"},
              {0, 1, 2, 41842148, "a b"},
              {0, 1618, 2, 93244, "sup"},
              {0, 1, 2, 9324, "suc"},
            }
        );

        // all edges from node 0, but take only one from each assoc list
        assert_eq(graph->assoc_range(0, -1, -1, 1),
            { {0, 2, 0, 9324, "succinct is cool"},
              {0, 1, 2, 41842148, "a b"}
            }
        );

        // all edges from node 0, for each assoc list, start from 2nd latest
        assert_eq(graph->assoc_range(0, -1, 1, -1),
            { {0, 1618, 2, 93244, "sup"},
              {0, 1, 2, 9324, "suc"},
            }
        );

        // all edges of assoc type 1
        assert_eq(graph->assoc_range(-1, 1, -1, -1),
            { {6, 1, 1, 111111, "abcd"} }
        );

        // all edges in  graph (wildcard all four arguments)
        assert_eq(graph->assoc_range(-1, -1, -1, -1),
            { {0, 2, 0, 9324, "succinct is cool"},
              {0, 1, 2, 41842148, "a b"},
              {0, 1618, 2, 93244, "sup"},
              {0, 1, 2, 9324, "suc"},
              {6, 1, 1, 111111, "abcd"}
            }
        );

        SuccinctGraph::print_assoc_results(graph->assoc_range(0, 0, 0, 1));

        assert_eq(graph->assoc_range(0, 0, 0, 1),
            { {0, 2, 0, 9324, "succinct is cool"} });

        SuccinctGraph::print_assoc_results(graph->assoc_range(0, 2, 0, 2));

        assert_eq(graph->assoc_range(0, 2, 0, 2),
            { {0, 1, 2, 41842148, "a b"},
              {0, 1618, 2, 93244, "sup"} });

        SuccinctGraph::print_assoc_results(graph->assoc_range(0, 2, 2, 1));

        assert_eq(graph->assoc_range(0, 2, 2, 1),
            { {0, 1, 2, 9324, "suc"} });

        SuccinctGraph::print_assoc_results(graph->assoc_range(6, 1, 0, 1));

        assert_eq(graph->assoc_range(6, 1, 0, 1),
            { {6, 1, 1, 111111, "abcd"} });

        // assoc_count() tests

        printf("assoc_count(0, 0) = %llu\n", graph->assoc_count(0, 0)); // 1
        printf("assoc_count(0, 2) = %llu\n", graph->assoc_count(0, 2)); // 3
        printf("assoc_count(6, 1) = %llu\n", graph->assoc_count(6, 1)); // 1

        assert(graph->assoc_count(0, 0) == 1);
        assert(graph->assoc_count(0, 2) == 3);
        assert(graph->assoc_count(6, 1) == 1);

        // count all edges
        assert(graph->assoc_count(-1, -1) == 5);

        // count all edges with a particular assoc type
        assert(graph->assoc_count(-1, 2) == 3 &&
               graph->assoc_count(-1, 0) == 1 &&
               graph->assoc_count(-1, 1) == 1);

        // count all edges that start from node 0
        assert(graph->assoc_count(0, -1) == 4);

        // count edges from non-existent node or that has non-existent atype
        assert(graph->assoc_count(1618, -1) == 0 &&
               graph->assoc_count(-1, 1618) == 0);

        // assoc_get() tests

        std::set<int64_t> dst_id_set;
        dst_id_set.insert(1618);

        SuccinctGraph::print_assoc_results(
            graph->assoc_get(0, 2, dst_id_set, 9324, 93245));
        assert_eq(graph->assoc_get(0, 2, dst_id_set, 9324, 93245),
            { {0, 1618, 2, 93244, "sup"} });

        dst_id_set.insert(1);
        SuccinctGraph::print_assoc_results(
            graph->assoc_get(0, 2, dst_id_set, 9324, 93245));
        assert_eq(graph->assoc_get(0, 2, dst_id_set, 9324, 93245),
            { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

        // no time lower bound (set to wildcard)
        assert_eq(graph->assoc_get(0, 2, dst_id_set, -1, 93245),
            { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

        // no time upper bound (set to wildcard)
        assert_eq(graph->assoc_get(0, 2, dst_id_set, 9324, -1),
            { {0, 1, 2, 41842148, "a b"},
              {0, 1618, 2, 93244, "sup"},
              {0, 1, 2, 9324, "suc"}
            });

        // all edges no earlier than time 100000 (with dst in { 1, 1618 })
        assert_eq(graph->assoc_get(-1, -1, dst_id_set, 100000, -1),
            { {0, 1, 2, 41842148, "a b"}, {6, 1, 1, 111111, "abcd"} });

        // all edges no later than time 100000 (with dst in { 1, 1618 })
        assert_eq(graph->assoc_get(-1, -1, dst_id_set, -1, 100000),
            { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

        // TODO: add proper Assoc and AType to other query results
        // assoc_time_range() tests

        // [id=1,time=111111,attr='abcd']
        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(6, 1, 1, 99999999, 10)); // 1 edge

//        assert_eq(graph->assoc_time_range(6, 1, 1, 99999999, 10),
//            { {6, 1, 1, 111111, "abcd"} });

//        SuccinctGraph::print_assoc_results(
//            graph->assoc_time_range(0, 0, 0, 1, 10)); // 0 edge // FIXME

        // [id=1618,time=93244,attr='sup'] [id=1,time=9324,attr='suc']
        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, 900, 93244, 2)); // 2 edges

        // [id=1618,time=93244,attr='sup']
        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, 900, 93244, 1)); // 1 edge

        // [id=1,time=41842148,attr='a b']
        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, -1, 99999999999, 1)); // 1 edge

        // [id=1,time=41842148,attr='a b'] [id=1618,time=93244,attr='sup'] [id=1,time=9324,attr='suc']
        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, -1, 99999999999, 100)); // 3 edges
    } else if (type == "old-api") {
        std::string node_succinct_dir = succinct_dir;
        std::string edge_succinct_dir = std::string(argv[optind + 1]);
        graph = new SuccinctGraph(succinct_dir, true); // no-op
        graph->load(node_succinct_dir, edge_succinct_dir);

        std::vector<int64_t> nbhrs;

        graph->get_neighbors(nbhrs, 0);
        print_vector("neighbors of node 0: ", nbhrs);

        graph->get_neighbors(nbhrs, 6);
        print_vector("neighbors of node 6: ", nbhrs);

        // for toy dataset, this will hit every nbhr of 0
        graph->get_neighbors(nbhrs, 0, 0, "5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE");
        print_vector("getNeibors(0, attr_that_will_hit): ", nbhrs);

        graph->get_neighbors(nbhrs, 6, 0, "5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE");
        print_vector("getNeibors(6, attr_that_will_hit): ", nbhrs);

        graph->get_neighbors(nbhrs, 0, 0, "WILL NOT HIT");
        print_vector("getNeibors(0, attr_that_won't_hit): ", nbhrs);

        std::set<int64_t> nodes;
        graph->get_nodes(nodes, 0, "5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE");
        assert(nodes.size() == 10);
        printf("get_nodes(attr that will hit) returns all nodes: ok\n");

        graph->get_nodes(nodes, 0, "WILL NOT HIT");
        assert(nodes.size() == 0);
        printf("get_nodes(attr that won't hit) returns no nodes: ok\n");

        graph->get_nodes(nodes,
            0, "5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE",
            8, "lnTipx7wXZAqJZR5Y4M9k8AIyGE9CpuX");
        assert(nodes.size() == 10);
        printf("get_nodes(attr1 (hit), attr2 (hit)) returns all nodes: ok\n");

        graph->get_nodes(nodes,
            0, "5PN2qmWqBlQ9wQj99nsQzldVI5ZuGXbE",
            8, "WILL NOT HIT");
        assert(nodes.size() == 0);
        printf("get_nodes(attr1 (hit), attr2 (no hit)) returns no nodes: ok\n");

    } else {
        assert(0); // Not supported
    }

    return 0;
}
