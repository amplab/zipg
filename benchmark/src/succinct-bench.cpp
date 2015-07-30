#include <cassert>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "succinct-graph/GraphFormatter.hpp"
#include "succinct-graph/SuccinctGraph.hpp"
#include "succinct-graph/utils.h"
#include "../include/GraphBenchmark.hpp"

void print_usage(char *exec) {
    fprintf(stderr, "Usage: %s [-t type] [-x warmup_n] [-y measure_n] [-w warmup_file] [-q query_file] [-a neighbor_warmup ] [-b neighbor_query] [-o output_file] [succinct_dir]\n", exec);
}

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
    std::initializer_list<int64_t> expected) {

    assert(expected.size() == actual.size());
    int i = 0;
    for (int64_t expected_elem : expected) {
        assert(actual[i] == expected_elem);
        ++i;
    }
}

void assert_eq(
    const std::set<int64_t>& actual,
    std::initializer_list<int64_t> expected) {

    assert(expected.size() == actual.size());
    std::vector<int64_t> vec;
    for (auto it = actual.begin(); it != actual.end(); ++it)
        vec.push_back(*it);
    assert_eq(vec, expected);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage(argv[0]);
        return -1;
    }

    int c;
    std::string type = "neighbor-throughput";
    int warmup_n = 20000; int measure_n = 100000;
    std::string warmup_query_file("warmup.txt");
    std::string measure_query_file("query_file.txt");
    std::string warmup_neighbor_file("warmup.txt");
    std::string measure_neighbor_file("query_file.txt");
    std::string result_file_name("benchmark_results.txt");

    std::string warmup_nhbr_node_file, nhbr_node_file;
    std::string warmup_node_file, query_node_file;
    std::string nhbr_atype_res, nhbr_node_res, node_res, node_node_res;

    int throughput_threads = 1;

    // TODO: how the script uses these here is a mess.
    while ((c = getopt(
        argc, argv, "t:x:y:z:w:q:a:b:c:d:e:f:o:h:i:j:k:p:")) != -1)
    {
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
        case 'c':
            warmup_nhbr_node_file = std::string(optarg);
            break;
        case 'd':
            nhbr_node_file = std::string(optarg);
            break;
        case 'e':
            warmup_node_file = std::string(optarg);
            break;
        case 'f':
            query_node_file = std::string(optarg);
            break;
        case 'o':
            result_file_name = std::string(optarg);
            break;
        case 'h':
            nhbr_atype_res = std::string(optarg);
            break;
        case 'i':
            nhbr_node_res = std::string(optarg);
            break;
        case 'j':
            node_res = std::string(optarg);
            break;
        case 'k':
            node_node_res = std::string(optarg);
            break;
        case 'p':
            throughput_threads = std::stoi(optarg);
            break;
        }
    }

    if (optind == argc) {
        print_usage(argv[0]);
        return -1;
    }

    // specified after all the options
    std::string node_file = std::string(argv[optind]);
    std::string edge_file = std::string(argv[optind + 1]);

    std::ofstream result_file(result_file_name, std::ios_base::app);
    SuccinctGraph* graph = nullptr;
    GraphBenchmark* bench = nullptr;

    bool is_sharded = (optind + 2 < argc); // if there exists a last dummy arg
    if (!is_sharded) {
        graph = new SuccinctGraph(node_file, edge_file); // loads
        bench = new GraphBenchmark(graph);
    } else {
        bench = new GraphBenchmark(nullptr); // constructs
    }

    if (type == "neighbor-latency") {

        bench->benchmark_neighbor_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);

    } else if (type == "node-latency") {

        bench->benchmark_node_latency(result_file_name, warmup_n, measure_n,
                warmup_query_file, measure_query_file);

    } else if (type == "neighbor-throughput") {

        int64_t warmup_microsecs = 60 * 1000 * 1000; // 1 min
        int64_t measure_microsecs = 120 * 1000 * 1000; // 2 min
        int64_t cooldown_microsecs = 5 * 1000 * 1000; // 5 sec

        bench->benchmark_neighbor_throughput(
            throughput_threads,
            warmup_microsecs,
            measure_microsecs,
            cooldown_microsecs,
            warmup_query_file,
            measure_query_file);

    } else if (type == "node-throughput") {

//        double thput = bench->benchmark_node_throughput(
//            warmup_query_file, measure_query_file);
//        result_file << "Get Name Throughput: " << thput << "\n\n";

    } else if (type == "mix-throughput") {

//        double thput = bench->benchmark_mix_throughput(
//                warmup_neighbor_file, measure_neighbor_file,
//                warmup_query_file, measure_query_file);
//        result_file << "Mix throughput: " << thput << "\n\n";

    } else if (type == "mix-latency") {

        bench->benchmark_mix_latency(result_file_name, // nhbr
            nhbr_atype_res, nhbr_node_res, node_res, node_node_res,
            warmup_n, measure_n,
            warmup_neighbor_file, measure_neighbor_file,
            warmup_query_file, measure_query_file, // nhbr_atype
            warmup_nhbr_node_file, nhbr_node_file,
            warmup_node_file, query_node_file);

    } else if (type == "node-node-latency") {

        bench->benchmark_node_node_latency(
            result_file_name, warmup_n, measure_n,
            warmup_query_file, measure_query_file);

    } else if (type == "neighbor-node-latency") {

        bench->benchmark_neighbor_node_latency(
            result_file_name,
            warmup_n,
            measure_n,
            warmup_query_file,
            measure_query_file);

    } else if (type == "neighbor-atype-latency") {

        bench->benchmark_neighbor_atype_latency(
            result_file_name,
            warmup_n,
            measure_n,
            warmup_query_file,
            measure_query_file);

    } else if (type == "graph-format") {

        GraphFormatter::format_node_file(node_file);
        printf("Node file formatted\n");

    } else if (type == "graph-test") {
        // TODO: use gtest & move this to standalone file at some point?

        std::string node_file_content =
            GraphFormatter::format_node_attrs_str( { { "" } } ); // empty
        std::string edge_file_content = "0 1 2 41842148 a b\n"
                                        "0 1618 2 93244 sup\n"
                                        "0 1 2 9324 suc\n"
                                        "0 2 0 9324 succinct is cool\n"
                                        "6 1 1 111111 abcd\n";

        std::string node_tmp_pathname = std::tmpnam(NULL);
        std::string edge_tmp_pathname = std::tmpnam(NULL);

        std::FILE* node_tmp_file = std::fopen(node_tmp_pathname.c_str(), "w+");
        std::FILE* edge_tmp_file = std::fopen(edge_tmp_pathname.c_str(), "w+");

        std::fputs(node_file_content.c_str(), node_tmp_file);
        std::fputs(edge_file_content.c_str(), edge_tmp_file);

        std::fclose(node_tmp_file);
        std::fclose(edge_tmp_file);

        printf("node tmp: %s\nedge tmp %s\n",
            node_tmp_pathname.c_str(), edge_tmp_pathname.c_str());

        graph->construct(node_tmp_pathname, edge_tmp_pathname);

        std::remove(node_tmp_pathname.c_str());
        std::remove(edge_tmp_pathname.c_str());

        printf("SuccinctGraph constructed.\n");

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

        printf("assoc_count(0, 0) = %lld\n", graph->assoc_count(0, 0)); // 1
        printf("assoc_count(0, 2) = %lld\n", graph->assoc_count(0, 2)); // 3
        printf("assoc_count(6, 1) = %lld\n", graph->assoc_count(6, 1)); // 1

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

        // assoc_time_range() tests

        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(6, 1, 1, 99999999, 10)); // 1 edge

        assert_eq(graph->assoc_time_range(6, 1, 1, 99999999, 10),
            { {6, 1, 1, 111111, "abcd"} });

        // nothing
        assert_eq(graph->assoc_time_range(0, 0, 0, 1, 10), { });

        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, 900, 93244, 2)); // 2 edges
        assert_eq(graph->assoc_time_range(0, 2, 900, 93244, 2),
            { {0, 1618, 2, 93244, "sup"}, {0, 1, 2, 9324, "suc"} });

        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, 900, 93244, 1)); // 1 edge
        assert_eq(graph->assoc_time_range(0, 2, 900, 93244, 1),
            { {0, 1618, 2, 93244, "sup"} });

        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, -1, 99999999999, 1)); // 1 edge
        assert_eq(graph->assoc_time_range(0, 2, -1, 99999999999, 1),
            { {0, 1, 2, 41842148, "a b"} });

        SuccinctGraph::print_assoc_results(
            graph->assoc_time_range(0, 2, -1, 99999999999, 100)); // 3 edges
        assert_eq(graph->assoc_time_range(0, 2, -1, 99999999999, 100),
            { {0, 1, 2, 41842148, "a b"},
              {0, 1618, 2, 93244, "sup"},
              {0, 1, 2, 9324, "suc"}
            });

        // all edges <= time 999999999; at most 2 edges from each assoc list
        assert_eq(graph->assoc_time_range(-1, -1, -1, 999999999, 2),
            { {0, 2, 0, 9324, "succinct is cool"},
              {0, 1, 2, 41842148, "a b"},
              {0, 1618, 2, 93244, "sup"},
              {6, 1, 1, 111111, "abcd"}
            }
        );

        // regression bug
        assert_eq(graph->assoc_time_range(-1, -1, 9324, 9324, -1),
            { {0, 2, 0, 9324, "succinct is cool"},
              {0, 1, 2, 9324, "suc"} });

        // primitive API tests

        std::vector<int64_t> nbhrs;

        graph->get_neighbors(nbhrs, 0);
        assert_eq(nbhrs, { 2, 1, 1618, 1 });

        graph->get_neighbors(nbhrs, 6);
        assert_eq(nbhrs, { 1 });

        graph->remove_generated_files();

    } else if (type == "graph-test2") {

        // only first 3 nodes have attrs, diff # of attrs, diff size
        std::string node_file_content = GraphFormatter::format_node_attrs_str(
            { { "Winter", "is", "coming" },
              { "is", "Winter", "here" },
              { "George", "R", "R", "Martin", "writes", "too", "damn", "slow" }
            });

        std::string edge_file_content = "0 1 2 41842148 a b\n"
                                        "0 1618 2 93244 sup\n"
                                        "0 1 2 9324 suc\n"
                                        "0 2 0 9324 succinct is cool\n"
                                        "6 1 1 111111 abcd\n";

        std::string node_tmp_pathname = std::tmpnam(NULL);
        std::string edge_tmp_pathname = std::tmpnam(NULL);
        std::FILE* node_tmp_file = std::fopen(node_tmp_pathname.c_str(), "w+");
        std::FILE* edge_tmp_file = std::fopen(edge_tmp_pathname.c_str(), "w+");
        std::fputs(node_file_content.c_str(), node_tmp_file);
        std::fputs(edge_file_content.c_str(), edge_tmp_file);
        std::fclose(node_tmp_file);
        std::fclose(edge_tmp_file);
        printf("node tmp: %s\nedge tmp %s\n",
            node_tmp_pathname.c_str(), edge_tmp_pathname.c_str());

        graph->construct(node_tmp_pathname, edge_tmp_pathname);

        std::vector<int64_t> nbhrs;
        std::set<int64_t> nodes;

        graph->get_neighbors(nbhrs, 6);
        assert_eq(nbhrs, { 1 });

        // get_nhbr(n, atype)
        graph->get_neighbors(nbhrs, 0, 2);
        assert_eq(nbhrs, { 1, 1618, 1 });

        graph->get_neighbors(nbhrs, 0, 3);
        assert_eq(nbhrs, { });

        graph->get_neighbors(nbhrs, 6, 1);
        assert_eq(nbhrs, { 1 });

        // several regression tests: test exact match semantics
        graph->get_neighbors(nbhrs, 0, 0, "Win"); // just a prefix of the attr!
        assert(nbhrs.empty());

        graph->get_nodes(nodes, 0, "Geo"); // just a prefix!
        assert(nodes.empty());

        // attr 0 prefix, attr 1 complete key
        graph->get_nodes(nodes, 0, "Win", 1, "is");
        assert(nodes.empty());

        // last attribute for a node, test end-of-record delim
        graph->get_nodes(nodes, 2, "com");
        assert(nodes.empty());
        graph->get_nodes(nodes, 7, "slo");
        assert(nodes.empty());
        graph->get_nodes(nodes, 7, "slow");
        assert(nodes.size() == 1 && *(nodes.begin()) == 2);

        graph->get_nodes(nodes, 0, "Winter");
        assert_eq(nodes, { 0 });

        graph->get_nodes(nodes, 1, "is not");
        assert_eq(nodes, { });

        graph->get_nodes(nodes, 1, "R");
        assert_eq(nodes, { 2 });

        graph->get_nodes(nodes, 7, "slow");
        assert_eq(nodes, { 2 });

        graph->get_nodes(nodes, 0, "George", 3, "Martin");
        assert_eq(nodes, { 2 });

        graph->get_neighbors(nbhrs, 0, 1, "Winter");
        assert_eq(nbhrs, { 1, 1 });

        graph->get_neighbors(nbhrs, 0, 0, "George");
        assert_eq(nbhrs, { 2 });

        graph->get_neighbors(nbhrs, 0, 7, "slow");
        assert_eq(nbhrs, { 2 });

        std::remove(node_tmp_pathname.c_str());
        std::remove(edge_tmp_pathname.c_str());
        graph->remove_generated_files();

    } else if (type == "demo") {

        printf("Loaded SuccinctGraph from files.\n\n");
        // Demo code

    } else {
        assert(0);
    }
    return 0;
}
