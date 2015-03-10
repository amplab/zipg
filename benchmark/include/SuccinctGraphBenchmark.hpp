#ifndef SUCCINCT_GRAPH_BENCHMARK_H
#define SUCCINCT_GRAPH_BENCHMARK_H

#include <cstdio>
#include <fstream>
#include <vector>

#include <sys/time.h>

#include "../../include/succinct-graph/SuccinctGraph.hpp"

#if defined(__i386__)

static __inline__ unsigned long long rdtsc(void) {
    unsigned long long int x;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (x));
    return x;
}
#elif defined(__x86_64__)

static __inline__ unsigned long long rdtsc(void) {
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

#elif defined(__powerpc__)

static __inline__ unsigned long long rdtsc(void) {
    unsigned long long int result=0;
    unsigned long int upper, lower,tmp;
    __asm__ volatile(
                "0:                  \n"
                "\tmftbu   %0           \n"
                "\tmftb    %1           \n"
                "\tmftbu   %2           \n"
                "\tcmpw    %2,%0        \n"
                "\tbne     0b         \n"
                : "=r"(upper),"=r"(lower),"=r"(tmp)
                );
    result = upper;
    result = result<<32;
    result = result|lower;

    return(result);
}

#else

#error "No tick counter is available!"

#endif


class SuccinctGraphBenchmark {

private:
    typedef unsigned long long int time_t;
    typedef unsigned long count_t;

    const count_t WARMUP_N = 1000;
    const count_t COOLDOWN_N = 1000;
    const count_t MEASURE_N = 10000;

    static const count_t WARMUP_T = 60000000;
    static const count_t MEASURE_T = 60000000;
    static const count_t COOLDOWN_T = 10000000;

    static time_t get_timestamp() {
		struct timeval now;
		gettimeofday (&now, NULL);

		return  now.tv_usec + (time_t)now.tv_sec * 1000000;
	}

    void generate_randoms() {
        count_t q_cnt = WARMUP_N + COOLDOWN_N + MEASURE_N;
        for(count_t i = 0; i < q_cnt; i++) {
            randoms.push_back(rand() % graph->num_nodes());
        }
    }

    void read_queries(std::string filename) {
        std::ifstream inputfile(filename);
        if(!inputfile.is_open()) {
            fprintf(stderr, "Error: Query file [%s] may be missing.\n",
                    filename.c_str());
            return;
        }

        std::string line, bin, query;
        while (getline(inputfile, line)) {
            // Extract key and value
            int split_index = line.find_first_of('\t');
            bin = line.substr(0, split_index);
            query = line.substr(split_index + 1);
            queries.push_back(query);
        }
        inputfile.close();
    }

public:

    SuccinctGraphBenchmark(SuccinctGraph *graph, std::string queryfile = "") {
        this->graph = graph;
        generate_randoms();
        if(queryfile != "") {
            read_queries(queryfile);
        }
    }

    std::pair<double, double> benchmark_neighbor_throughput() {
        double get_neighbor_thput = 0;
        double edges_thput = 0;
        std::string value;

        try {
            // Warmup phase
            long i = 0;
            time_t warmup_start = get_timestamp();
            while (get_timestamp() - warmup_start < WARMUP_T) {
                graph->get_neighbors(value, randoms[i % randoms.size()]);
                i++;
            }

            // Measure phase
            i = 0;
            long edges = 0;
            double totsecs = 0;
            time_t start = get_timestamp();
            while (get_timestamp() - start < MEASURE_T) {
                time_t query_start = get_timestamp();
                graph->get_neighbors(value, randoms[i % randoms.size()]);
                time_t query_end = get_timestamp();
                totsecs += (double) (query_end - query_start) / (1000.0 * 1000.0);
                edges += std::count(value.begin(), value.end(), ' ');
                i++;
            }
            get_neighbor_thput = ((double) i / totsecs);
            edges_thput = ((double) edges / totsecs);

            i = 0;
            time_t cooldown_start = get_timestamp();
            while (get_timestamp() - cooldown_start < COOLDOWN_T) {
                graph->get_neighbors(value, randoms[i % randoms.size()]);
                i++;
            }

        } catch (std::exception &e) {
            fprintf(stderr, "Throughput test ends...\n");
        }

        return std::make_pair(get_neighbor_thput, edges_thput);
    }

private:
    std::vector<uint64_t> randoms;
    std::vector<std::string> queries;
    SuccinctGraph *graph;
};

#endif
