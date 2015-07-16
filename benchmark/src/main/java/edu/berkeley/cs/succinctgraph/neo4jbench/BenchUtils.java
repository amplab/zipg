package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.schema.Schema;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BenchUtils {

    public static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public static void awaitIndexes(GraphDatabaseService graphDb) {
        Schema schema = graphDb.schema();
        try {
            schema.awaitIndexesOnline(60, TimeUnit.SECONDS);
            System.out.println("Indexes online");
        } catch (IllegalStateException e) {
            System.err.println("Indexes not all online after 60 seconds: " + e.getMessage());
            throw e;
        }
    }

    public static <T> T modGet(List<T> xs, int i) {
        return xs.get(i % xs.size());
    }

    public static <T> void print(String header, List<T> xs, PrintWriter out) {
        out.println(header);
        for (T x : xs) {
            out.printf("%s ", x);
        }
        out.println();
    }

    public static void printMemoryFootprint() {
        Runtime rt = Runtime.getRuntime();
        long max = rt.maxMemory();
        long allocated = rt.totalMemory();
        System.out.printf("JVM memory: Max %d, Allocated %d, Used %d\n",
            max, allocated, allocated - rt.freeMemory());
    }

}
