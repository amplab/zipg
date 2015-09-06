package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.*;

public class BenchNode {

    private static int WARMUP_N = 100000;
    private static int MEASURE_N = 100000;

    private static final Label label = DynamicLabel.label("Node");

    private static List<Integer> warmupAttributes1 = new ArrayList<Integer>();
    private static List<Integer> warmupAttributes2 = new ArrayList<Integer>();
    private static List<String> warmupQueries1 = new ArrayList<String>();
    private static List<String> warmupQueries2 = new ArrayList<String>();
    private static List<Integer> attributes1 = new ArrayList<Integer>();
    private static List<Integer> attributes2 = new ArrayList<Integer>();
    private static List<String> queries1 = new ArrayList<String>();
    private static List<String> queries2 = new ArrayList<String>();

    public static void main(String[] args) {
        String type = args[0];
        String dbPath = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String outputFile = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        int numClients = Integer.parseInt(args[7]);
        boolean tuned = Boolean.valueOf(args[8]);

        String neo4jPageCacheMem;
        if (args.length >= 10) {
            neo4jPageCacheMem = args[9];
        } else {
            neo4jPageCacheMem = GraphDatabaseSettings.pagecache_memory
                .getDefaultValue();
        }

        BenchUtils.getNodeQueries(warmup_query_path, warmupAttributes1,
            warmupAttributes2, warmupQueries1, warmupQueries2);
        BenchUtils.getNodeQueries(query_path, attributes1, attributes2,
            queries1, queries2);

        if (type.equals("node-throughput")) {
            nodeThroughput(tuned, dbPath, neo4jPageCacheMem, numClients);
        } else if (type.equals("node-latency")) {
            nodeLatency(tuned, dbPath, neo4jPageCacheMem, outputFile);
        } else if (type.equals("node-node-latency")) {
            nodeNodeLatency(tuned, dbPath, neo4jPageCacheMem, outputFile);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    private static void nodeLatency(boolean tuned,
        String dbPath, String neo4jPageCacheMem, String output_file) {

        System.out.println("Benchmarking getNode queries");
        System.out.println("Setting neo4j's dbms.pagecache.memory: " + neo4jPageCacheMem);

        GraphDatabaseService graphDb;
        if (tuned) {
            graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbPath)
                .setConfig(GraphDatabaseSettings.cache_type, "none")
                .setConfig(
                    GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
                .newGraphDatabase();
        } else {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        }

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
            if (tuned) {
                fullWarmup(graphDb);
            }

            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                Set<Long> nodes = getNodes(graphDb, label,
                    modGet(warmupAttributes1, i),
                    modGet(warmupQueries1, i));
                if (nodes.size() == 0) {
                    System.out.println("Error: no results for attribute " +
                        modGet(warmupAttributes1, i) + ", searching for " +
                        modGet(warmupQueries1, i));
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
                int attr = modGet(attributes1, i);
                String query = modGet(queries1, i);
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

    private static void nodeNodeLatency(boolean tuned,
        String dbPath, String neo4jPageCacheMem, String output_file) {

        System.out.println("Benchmarking getNodeNode queries");

        GraphDatabaseService graphDb;
        if (tuned) {
            graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbPath)
                .setConfig(GraphDatabaseSettings.cache_type, "none")
                .setConfig(
                    GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
                .newGraphDatabase();
        } else {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        }

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
            if (tuned) {
                fullWarmup(graphDb);
            }

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

    static class RunNodeThroughput implements Runnable {
        private int clientId;
        private GraphDatabaseService graphDb;

        public RunNodeThroughput(
            int clientId, GraphDatabaseService graphDb) {

            this.clientId = clientId;
            this.graphDb = graphDb;
        }

        public void run() {
            Transaction tx = graphDb.beginTx();
            PrintWriter out = null;
            Random rand = new Random(1618 + clientId);
            try {
                // true for append
                out = new PrintWriter(new BufferedWriter(new FileWriter(
                    "neo4j_throughput_get_nodes.txt", true)));

                // warmup
                int i = 0, queryIdx, warmupSize = warmupAttributes1.size();
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(warmupSize);
                    getNodes(graphDb, label,
                        modGet(warmupAttributes1, queryIdx),
                        modGet(warmupQueries1, queryIdx));
                    ++i;
                }

                // measure
                i = 0;
                long totalNodes = 0;
                int querySize = attributes1.size();
                Set<Long> nodes;
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(querySize);
                    nodes = getNodes(graphDb, label,
                        modGet(attributes1, queryIdx),
                        modGet(queries1, queryIdx));
                    totalNodes += nodes.size();
                    ++i;
                }
                long end = System.nanoTime();
                double totalSeconds = (end - start) * 1. / 1e9;
                double queryThput = ((double) i) / totalSeconds;
                double nodeThput = ((double) totalNodes) / totalSeconds;

                // cooldown
                long cooldownStart = System.nanoTime();
                while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                    getNodes(graphDb, label,
                        modGet(attributes1, i),
                        modGet(queries1, i));
                    ++i;
                }
                out.printf("%d %d\n", (int) queryThput, (int) nodeThput);

            } catch (Exception e) {
                System.err.printf("Client %d throughput bench exception: %s\n",
                    clientId, e);
                System.exit(1);
            } finally {
                if (out != null) {
                    out.close();
                }
                tx.success();
                tx.close();
            }
        }
    }

    private static void nodeThroughput(boolean tuned,
        String dbPath,String neo4jPageCacheMem, int numClients) {

        GraphDatabaseService graphDb;
        if (tuned) {
            graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbPath)
                .setConfig(GraphDatabaseSettings.cache_type, "none")
                .setConfig(
                    GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
                .newGraphDatabase();
        } else {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        }

        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = null;

        try {
            tx = graphDb.beginTx();
            if (tuned) {
                BenchUtils.fullWarmup(graphDb);
            }
            BenchUtils.awaitIndexes(graphDb);
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }

        try {
            List<Thread> clients = new ArrayList<>(numClients);
            for (int i = 0; i < numClients; ++i) {
                clients.add(new Thread(new RunNodeThroughput(i, graphDb)));
            }
            for (Thread thread : clients) {
                thread.start();
            }
            for (Thread thread : clients) {
                thread.join();
            }
        } catch (Exception e) {
            System.err.printf("Benchmark throughput exception: %s\n", e);
            System.exit(1);
        } finally {
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
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
