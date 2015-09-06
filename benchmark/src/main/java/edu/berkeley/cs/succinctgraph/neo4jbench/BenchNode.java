package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.*;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;

public class BenchNode {

    private static int WARMUP_N = 100000;
    private static int MEASURE_N = 100000;

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        String neo4jPageCacheMemory;
        if (args.length >= 8) {
            neo4jPageCacheMemory = args[7];
        } else {
            neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory
                .getDefaultValue();
        }

        List<Integer> warmupAttributes1 = new ArrayList<Integer>();
        List<Integer> warmupAttributes2 = new ArrayList<Integer>();
        List<String> warmupQueries1 = new ArrayList<String>();
        List<String> warmupQueries2 = new ArrayList<String>();
        List<Integer> attributes1 = new ArrayList<Integer>();
        List<Integer> attributes2 = new ArrayList<Integer>();
        List<String> queries1 = new ArrayList<String>();
        List<String> queries2 = new ArrayList<String>();

        BenchUtils.getNodeQueries(warmup_query_path, warmupAttributes1,
            warmupAttributes2, warmupQueries1, warmupQueries2);
        BenchUtils.getNodeQueries(query_path, attributes1, attributes2,
            queries1, queries2);

        if (type.equals("node-throughput")) {
            nodeThroughput(db_path, warmupAttributes1, warmupQueries1,
                attributes1, queries1, output_file);
        } else if (type.equals("node-latency")) {
            nodeLatency(db_path, neo4jPageCacheMemory, warmupAttributes1,
                warmupQueries1, attributes1, queries1, output_file);
        } else if (type.equals("node-node-latency")) {
            nodeNodeLatency(db_path, neo4jPageCacheMemory, warmupAttributes1,
                warmupAttributes2, warmupQueries1, warmupQueries2,
                attributes1, attributes2, queries1, queries2, output_file);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    private static void nodeLatency(
        String DB_PATH, String neo4jPageCacheMem,
        List<Integer> warmupAttributes, List<String> warmupQueries,
        List<Integer> attributes, List<String> queries, String output_file) {

        System.out.println("Benchmarking getNode queries");
        System.out.println("Setting neo4j's dbms.pagecache.memory: " + neo4jPageCacheMem);
        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(DB_PATH)
            .setConfig(GraphDatabaseSettings.cache_type, "none")
            .setConfig(
                GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
            .newGraphDatabase();
        BenchUtils.registerShutdownHook(graphDb);
        Label label = DynamicLabel.label("Node");
        Transaction tx = graphDb.beginTx();
        BenchUtils.awaitIndexes(graphDb);
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
            PrintWriter resOut = null;
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = new PrintWriter(new BufferedWriter(
                    new FileWriter(output_file + ".neo4j_result")));
            }

            // warmup
            fullWarmup(graphDb);

            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                Set<Long> nodes = getNodes(graphDb, label, modGet(warmupAttributes, i),
                    modGet(warmupQueries, i));
                if (nodes.size() == 0) {
                    System.out.println("Error: no results for attribute " +
                        modGet(warmupAttributes, i) + ", searching for " +
                        modGet(warmupQueries, i));
                    System.exit(0);
                }
            }

            // measure
            System.out.println("Measuring for " + MEASURE_N + " queries");
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                int attr = modGet(attributes, i);
                String query = modGet(queries, i);
                long queryStart = System.nanoTime();
                Set<Long> nodes = getNodes(graphDb, label, attr, query);
                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(nodes.size() + "," + microsecs);

                // correctness
                if (resOut != null) {
//                    Collections.sort(nodes);
//                    BenchUtils.print(String.format("attr %d: %s", attr, query), nodes, resOut);
                }
            }

            tx.success();
            out.close();
            if (resOut != null) resOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            printMemoryFootprint();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    private static void nodeNodeLatency(
        String DB_PATH, String neo4jPageCacheMemory,
        List<Integer> warmupAttributes1, List<Integer> warmupAttributes2,
        List<String> warmupQueries1, List<String> warmupQueries2,
        List<Integer> attributes1, List<Integer> attributes2,
        List<String> queries1, List<String> queries2, String output_file) {

        System.out.println("Benchmarking getNodeNode queries");
        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(DB_PATH)
            .setConfig(GraphDatabaseSettings.cache_type, "none")
            .setConfig(
                GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMemory)
            .newGraphDatabase();

        BenchUtils.registerShutdownHook(graphDb);
        Label label = DynamicLabel.label("Node");
        Transaction tx = graphDb.beginTx();
        BenchUtils.awaitIndexes(graphDb);
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
            PrintWriter resOut = null;
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = new PrintWriter(new BufferedWriter(
                    new FileWriter(output_file + ".neo4j_result")));
            }

            // warmup
            fullWarmup(graphDb);

            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                Set<Long> nodes = getNodes(graphDb, label,
                    modGet(warmupAttributes1, i),
                    modGet(warmupQueries1, i),
                    modGet(warmupAttributes2, i),
                    modGet(warmupQueries2, i));
                if (nodes.size() == 0) {
                    System.out.printf(
                        "Error: no results for attr1 %d search %s, attr2 %d, search %s\n",
                        modGet(warmupAttributes1, i), modGet(warmupQueries1, i),
                        modGet(warmupAttributes2, i), modGet(warmupQueries2, i));
                    System.exit(0);
                }
            }

            // measure
            System.out.println("Measuring for " + MEASURE_N + " queries");
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                long queryStart = System.nanoTime();
                Set<Long> nodes = getNodes(graphDb, label,
                    modGet(attributes1, i),
                    modGet(queries1, i),
                    modGet(attributes2, i),
                    modGet(queries2, i));
                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(nodes.size() + "," + microsecs);

                if (resOut != null) {
                    // correctness
                    String header = String.format("attr1 %d: %s; attr2 %d: %s",
                        modGet(attributes1, i),
                        modGet(queries1, i),
                        modGet(attributes2, i),
                        modGet(queries2, i));
//                    Collections.sort(nodes);
//                    BenchUtils.print(header, nodes, resOut);
                }
            }

            tx.success();
            out.close();
            if (resOut != null) resOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            printMemoryFootprint();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    private static void nodeThroughput(
        String DB_PATH,
        List<Integer> warmupAttributes, List<String> warmupQueries,
        List<Integer> attributes, List<String> queries, String output_file) {

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabase(DB_PATH);
        BenchUtils.registerShutdownHook(graphDb);
        Label label = DynamicLabel.label("Node");
        try (Transaction tx = graphDb.beginTx()) {
            BenchUtils.awaitIndexes(graphDb);

            // warmup
            int i = 0;
            System.out.println("Warming up queries");
            long warmupStart = System.nanoTime();
            while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                Set<Long> nodes = getNodes(graphDb, label, modGet(warmupAttributes, i),
                    modGet(warmupQueries, i));
                if (nodes.size() == 0) {
                    System.out.println("wtf " + modGet(warmupAttributes, i) + " " +
                        modGet(warmupQueries, i));
                    System.exit(0);
                }
                i++;
            }

            // measure
            i = 0;
            System.out.println("Measure queries");
            double totalSeconds = 0;
            long start = System.nanoTime();
            while (System.nanoTime() - start < MEASURE_TIME) {
                long queryStart = System.nanoTime();
                Set<Long> nodes = getNodes(graphDb, label, modGet(attributes, i),
                    modGet(queries, i));
                long queryEnd = System.nanoTime();
                totalSeconds += (queryEnd - queryStart) / 1E9;
                i++;
            }
            double thput = ((double) i) / totalSeconds;

            System.out.println("Get Name throughput: " + thput);
            try (PrintWriter out =
                     new PrintWriter(new BufferedWriter(new FileWriter(output_file, true)))) {
                out.println(DB_PATH);
                out.println(thput + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            tx.success();
        }
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    public static Set<Long> getNodes(GraphDatabaseService graphDb,
                                     Label label, int attr, String search) {

        ResourceIterator<Node> nodes = graphDb.findNodes(
            label, "name" + attr, search);
        try {
            Set<Long> userIds = new HashSet<Long>();
            while (nodes.hasNext()) {
                userIds.add(nodes.next().getId());
            }
            return userIds;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Set<Long> getNodes(
        GraphDatabaseService graphDb,
        Label label, int attr1, String search1, int attr2, String search2) {

        ResourceIterator<Node> nodes = graphDb.findNodes(
            label, "name" + attr1, search1);
        ResourceIterator<Node> nodes2 = graphDb.findNodes(
            label, "name" + attr2, search2);
        try {
            Set<Long> s1 = new HashSet<Long>();
            while (nodes.hasNext()) {
                s1.add(nodes.next().getId());
            }
            Set<Long> s2 = new HashSet<Long>();
            while (nodes2.hasNext()) {
                s2.add(nodes2.next().getId());
            }
            s1.retainAll(s2);
            return s1;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
