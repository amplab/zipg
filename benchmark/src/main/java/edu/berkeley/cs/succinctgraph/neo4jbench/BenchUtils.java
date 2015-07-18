package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    /** Scans through all data including edge properties and node properties. */
    public static void fullWarmup(GraphDatabaseService graphDb) {
        long start = System.nanoTime();
        Object prop;
        Iterable<Relationship> iter = GlobalGraphOperations.at(graphDb)
            .getAllRelationships();
        Set<String> propKeySet = new HashSet<String>();
        for (Relationship rel : iter) {
            RelationshipType relType = rel.getType();
            Iterable<String> propKeys = rel.getPropertyKeys();
            for (String key : propKeys) {
                prop = rel.getProperty(key, "");
                if (!propKeySet.contains(key)) {
                    // hopefully this prevents aggressive optimization
                    System.out.println(
                        "New edge prop key: " + key +
                        " value: " + prop +
                        " relType: " + relType);
                    propKeySet.add(key);
                }
            }
        }

        ResourceIterable<Node> nodes = GlobalGraphOperations.at(graphDb)
            .getAllNodes();
        for (Node node : nodes) {
            Iterable<String> propKeys = node.getPropertyKeys();
            for (String key : propKeys) {
                prop = node.getProperty(key, "");
                if (!propKeySet.contains(key)) {
                    // hopefully this prevents aggressive optimization
                    System.out.println(
                        "New node prop key: " + key +
                            " value: " + prop +
                            " node id: " + node.getId());
                    propKeySet.add(key);
                }
            }
        }
        long end = System.nanoTime();
        printMemoryFootprint();
        System.out.println(
            "Full warmup done in " + (end - start) / 1e6 + " millis");
    }

}
