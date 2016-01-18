#ifndef BENCHMARK_CONSTANTS_H
#define BENCHMARK_CONSTANTS_H

// FIXME: these are hard-coded hacks.
// Twitter
constexpr static int64_t NUM_NODES = 41652230;
constexpr static int64_t NUM_ATYPES = 5;
constexpr static int64_t MIN_TIME = 1439721981221; // unused
constexpr static int64_t MAX_TIME = 1441905687237;
const std::string ATTR_FOR_NEW_EDGES = std::string(128, '|');

const static int MAX_NUM_NEW_EDGES = 200000; // 3.5M takes too long to run

// Timings for throughput benchmarks.
constexpr static int64_t WARMUP_MICROSECS = 300 * 1000 * 1000;
constexpr static int64_t MEASURE_MICROSECS = 900 * 1000 * 1000;
constexpr static int64_t COOLDOWN_MICROSECS = 450 * 1000 * 1000;
//constexpr static int64_t WARMUP_MICROSECS = 60 * 1000 * 1000;
//constexpr static int64_t MEASURE_MICROSECS = 120 * 1000 * 1000;
//constexpr static int64_t COOLDOWN_MICROSECS = 30 * 1000 * 1000;

constexpr static int query_batch_size = 100;

typedef enum {
    NHBR = 0,
    NHBR_ATYPE = 1,
    NHBR_NODE = 2,
    NODE = 3, // deprecated
    NODE2 = 4,
    MIX = 5,
    TAO_MIX = 6,
    EDGE_ATTRS = 7,
    TAO_MIX_WITH_UPDATES = 8
} BenchType;

// Read workload distribution; from ATC 13 Bronson et al.
constexpr static double TAO_WRITE_PERC = 0.002;
constexpr static double ASSOC_RANGE_PERC = 0.409;
constexpr static double OBJ_GET_PERC = 0.289;
constexpr static double ASSOC_GET_PERC = 0.157;
constexpr static double ASSOC_COUNT_PERC = 0.117;
constexpr static double ASSOC_TIME_RANGE_PERC = 0.028;

inline int choose_query(double rand_r) {
    if (rand_r < ASSOC_RANGE_PERC) {
        return 0;
    } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
        return 1;
    } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
        return 2;
    } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC +
        ASSOC_GET_PERC + ASSOC_COUNT_PERC)
    {
        return 3;
    }
    return 4;
}

inline int choose_query_with_updates(double rand_update, double rand_r) {
    if (rand_update < TAO_WRITE_PERC) {
        return 5; // assoc_add only, for now
    }
    // otherwise, all masses are allocated to reads
    if (rand_r < ASSOC_RANGE_PERC) {
        return 0;
    } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
        return 1;
    } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
        return 2;
    } else if (rand_r < ASSOC_RANGE_PERC + OBJ_GET_PERC +
        ASSOC_GET_PERC + ASSOC_COUNT_PERC)
    {
        return 3;
    }
    return 4;
}

#endif
