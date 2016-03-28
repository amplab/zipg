package edu.berkeley.cs.succinctgraph.neo4jbench;

public class BenchConstants {

    // Timings for throughput benchmarks.
    public static final long WARMUP_TIME = (long) (60 * 1e9); // 1e9 = 1 sec
    public static final long MEASURE_TIME = (long) (120 * 1e9);
    public static final long COOLDOWN_TIME = (long) (5 * 1e9);

}
