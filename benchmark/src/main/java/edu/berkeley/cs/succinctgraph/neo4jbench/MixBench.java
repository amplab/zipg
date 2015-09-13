package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.*;

public class MixBench {
    private static int WARMUP_N;
    private static int MEASURE_N;

    static final Label label = DynamicLabel.label("Node");

    // get_nhbrs(n)
    static List<Long> warmupNhbrs = new ArrayList<>();
    static List<Long> nhbrs = new ArrayList<>();

    // get_nhbrs(n, attr)
    static List<Long> warmupNhbrNodeIds = new ArrayList<>();
    static List<Integer> warmupNhbrNodeAttrIds = new ArrayList<>();
    static List<String> warmupNhbrNodeAttrs = new ArrayList<>();
    static List<Long> nhbrNodeIds = new ArrayList<>();
    static List<Integer> nhbrNodeAttrIds = new ArrayList<>();
    static List<String> nhbrNodeAttrs = new ArrayList<>();

    // get_nhbrs(n, atype)
    static List<Long> warmupNhbrAtypeIds = new ArrayList<>();
    static List<Long> warmupNhbrAtypeAtypes = new ArrayList<>();
    static List<Long> nhbrAtypeIds = new ArrayList<>();
    static List<Long> nhbrAtypeAtypes = new ArrayList<>();

    // get_nodes(attr) and get_nodes(attr1, attr2)
    static List<Integer> warmupNodeAttrIds1 = new ArrayList<Integer>();
    static List<String> warmupNodeAttrs1 = new ArrayList<String>();
    static List<Integer> warmupNodeAttrIds2 = new ArrayList<Integer>();
    static List<String> warmupNodeAttrs2 = new ArrayList<String>();
    static List<Integer> nodeAttrIds1 = new ArrayList<Integer>();
    static List<String> nodeAttrs1 = new ArrayList<String>();
    static List<Integer> nodeAttrIds2 = new ArrayList<Integer>();
    static List<String> nodeAttrs2 = new ArrayList<String>();

    public static void main(String[] args) throws Exception {
        String type = args[0];
        String dbPath = args[1];
        String warmupNeighborFile = args[2];
        String queryNeighborFile = args[3];
        String warmupNhbrNodeFile = args[4];
        String queryNhbrNodeFile = args[5];
        String warmupNhbrAtypeFile = args[6];
        String queryNhbrAtypeFile = args[7];
        String warmupNodeFile = args[8];
        String queryNodeFile = args[9];

        String nhbrOut = args[10];
        String nhbrNodeOut = args[11];
        String nhbrAtypeOut = args[12];
        String nodeOut = args[13];
        String doubleNodeOut = args[14];

        WARMUP_N = Integer.parseInt(args[15]);
        MEASURE_N = Integer.parseInt(args[16]);
        int numClients = Integer.parseInt(args[17]);
        boolean tuned = Boolean.valueOf(args[18]);

        String neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length > 19) {
            neo4jPageCacheMemory = args[19];
        }

        // get_nhbrs(n)
        getNeighborQueries(warmupNeighborFile, warmupNhbrs);
        getNeighborQueries(queryNeighborFile, nhbrs);

        // get_nhbrs(n, attr)

        getNeighborNodeQueries(warmupNhbrNodeFile,
            warmupNhbrNodeIds, warmupNhbrNodeAttrIds, warmupNhbrNodeAttrs);
        getNeighborNodeQueries(queryNhbrNodeFile,
            nhbrNodeIds, nhbrNodeAttrIds, nhbrNodeAttrs);

        // get_nhbrs(n, atype)
        getNeighborAtypeQueries(warmupNhbrAtypeFile,
            warmupNhbrAtypeIds, warmupNhbrAtypeAtypes);
        getNeighborAtypeQueries(queryNhbrAtypeFile,
            nhbrAtypeIds, nhbrAtypeAtypes);

        // get_nodes(attr) and get_nodes(attr1, attr2)
        getNodeQueries(warmupNodeFile,
            warmupNodeAttrIds1, warmupNodeAttrIds2,
            warmupNodeAttrs1, warmupNodeAttrs2);
        getNodeQueries(queryNodeFile,
            nodeAttrIds1, nodeAttrIds2,
            nodeAttrs1, nodeAttrs2);

        // start!

        if (type.equals("latency")) {
            mixLatency(tuned, dbPath, neo4jPageCacheMemory,
                nhbrOut, nhbrNodeOut, nhbrAtypeOut, nodeOut, doubleNodeOut);
        } else if (type.equals("throughput")) {
            mixThroughput(tuned, dbPath, neo4jPageCacheMemory, numClients);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    private static void mixThroughput(
        boolean tuned, String dbPath, String neo4jPageCacheMem, int numClients) {

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
                clients.add(new Thread(new RunMixThroughput(i, graphDb)));
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
            BenchUtils.printMemoryFootprint();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    static class RunMixThroughput implements Runnable {
        private int clientId;
        private GraphDatabaseService graphDb;

        final int warmupNhbrSize = warmupNhbrs.size();
        final int nhbrSize = nhbrs.size();
        final int warmupNhbrNodeSize = warmupNhbrNodeIds.size();
        final int nhbrNodeSize = nhbrNodeIds.size();
        final int warmupNhbrAtypSize = warmupNhbrAtypeIds.size();
        final int nhbrAtypeSize = nhbrAtypeIds.size();
        final int warmupNodeSize = warmupNodeAttrIds1.size();
        final int nodeSize = nodeAttrIds1.size();

        public RunMixThroughput(
            int clientId, GraphDatabaseService graphDb) {

            this.clientId = clientId;
            this.graphDb = graphDb;
        }

        private void runWarmup(Random rand) {
            int randQuery = rand.nextInt(5), queryIdx;
            switch (randQuery) {
                case 0:
                    queryIdx = rand.nextInt(warmupNhbrSize);
                    BenchNeighbor.getNeighborsSorted(graphDb,
                        modGet(warmupNhbrs, queryIdx));
                    break;
                case 1:
                    queryIdx = rand.nextInt(warmupNhbrNodeSize);
                    NeighborNodeBench.getNeighborNode(graphDb,
                        modGet(warmupNhbrNodeIds, queryIdx),
                        modGet(warmupNhbrNodeAttrIds, queryIdx),
                        modGet(warmupNhbrNodeAttrs, queryIdx));
                    break;
                case 2:
                    queryIdx = rand.nextInt(warmupNodeSize);
                    BenchNode.getNodes(graphDb, label,
                        modGet(warmupNodeAttrIds1, queryIdx),
                        modGet(warmupNodeAttrs1, queryIdx));
                    break;
                case 3:
                    queryIdx = rand.nextInt(warmupNhbrAtypSize);
                    BenchNeighborAtype.getNeighborsSorted(graphDb,
                        modGet(warmupNhbrAtypeIds, queryIdx),
                        BenchNeighborAtype.atypeMap[modGet(
                            warmupNhbrAtypeAtypes,
                            queryIdx).intValue()]);
                    break;
                case 4:
                    queryIdx = rand.nextInt(warmupNodeSize);
                    BenchNode.getNodes(graphDb, label,
                        modGet(warmupNodeAttrIds1, queryIdx),
                        modGet(warmupNodeAttrs1, queryIdx),
                        modGet(warmupNodeAttrIds2, queryIdx),
                        modGet(warmupNodeAttrs2, queryIdx));
                    break;
            }
        }

        public void run() {
            Transaction tx = graphDb.beginTx();
            PrintWriter out = null;
            Random rand = new Random(1618 + clientId);
            int randQuery;

            try {
                // true for append
                out = new PrintWriter(new BufferedWriter(new FileWriter(
                    "neo4j_throughput_mix.txt", true)));

                // warmup
                int i = 0, queryIdx;
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    runWarmup(rand);
                }

                // measure
                i = 0;
                long edges = 0;
                List<Long> neighbors;
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    randQuery = rand.nextInt(5);
                    switch (randQuery) {
                        case 0:
                            // get_nhbrs(n)
                            queryIdx = rand.nextInt(nhbrSize);
                            neighbors = BenchNeighbor.getNeighborsSorted(
                                graphDb, modGet(nhbrs, queryIdx));
                            edges += neighbors.size();
                            break;
                        case 1:
                            // get_nhbrs(n, attr)
                            queryIdx = rand.nextInt(nhbrNodeSize);
                            neighbors = NeighborNodeBench.getNeighborNode(
                                graphDb,
                                modGet(nhbrNodeIds, queryIdx),
                                modGet(nhbrNodeAttrIds, queryIdx),
                                modGet(nhbrNodeAttrs, queryIdx));
                            edges += neighbors.size();
                            break;
                        case 2:
                            // get_nodes(attr)
                            queryIdx = rand.nextInt(nodeSize);
                            BenchNode.getNodes(graphDb,
                                label,
                                modGet(nodeAttrIds1, queryIdx),
                                modGet(nodeAttrs1, queryIdx));
                            break;
                        case 3:
                            // get_nhbrs(n, atype)
                            queryIdx = rand.nextInt(nhbrAtypeSize);
                            neighbors = BenchNeighborAtype.getNeighborsSorted(
                                graphDb,
                                modGet(nhbrAtypeIds, queryIdx),
                                BenchNeighborAtype.atypeMap[modGet(
                                    nhbrAtypeAtypes, queryIdx).intValue()]);
                            edges += neighbors.size();
                            break;
                        case 4:
                            // get_nodes(attr1, attr2)
                            queryIdx = rand.nextInt(nodeSize);
                            BenchNode.getNodes(graphDb, label,
                                modGet(nodeAttrIds1, queryIdx),
                                modGet(nodeAttrs1, queryIdx),
                                modGet(nodeAttrIds2, queryIdx),
                                modGet(nodeAttrs2, queryIdx));
                            break;
                    }
                    ++i;
                }
                long end = System.nanoTime();
                double totalSeconds = (end - start) * 1. / 1e9;
                double queryThput = ((double) i) / totalSeconds;
                double edgesThput = ((double) edges) / totalSeconds;

                // cooldown
                long cooldownStart = System.nanoTime();
                while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                    runWarmup(rand);
                }
                out.printf("%d %d\n", (int) queryThput, (int) edgesThput);

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

    /** Note: the mixing order can affect query performance. */
    private static void mixLatency(boolean tuned,
        String dbPath, String neo4jPageCacheMem,
        String nhbrOutFile, String nhbrNodeOutFile,
        String nhbrAtypeOutFile, String nodeOutFile, String doubleNodeOutFile) {

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
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter nhbrOut = new PrintWriter(new BufferedWriter(
                new FileWriter(nhbrOutFile)));
            PrintWriter nhbrNodeOut = new PrintWriter(new BufferedWriter(
                new FileWriter(nhbrNodeOutFile)));
            PrintWriter nhbrAtypeOut = new PrintWriter(new BufferedWriter(
                new FileWriter(nhbrAtypeOutFile)));
            PrintWriter nodeOut = new PrintWriter(new BufferedWriter(
                new FileWriter(nodeOutFile)));
            PrintWriter doubleNodeOut = new PrintWriter(new BufferedWriter(
                new FileWriter(doubleNodeOutFile)));

            if (tuned) {
                BenchUtils.fullWarmup(graphDb);
            }
            BenchUtils.awaitIndexes(graphDb);

            long seed = 1618L;
            Random rand = new Random(seed);
            int randQuery;

            // warmup
            System.out.printf("Warming up %d queries\n", WARMUP_N);
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                randQuery = rand.nextInt(5);
                switch (randQuery) {
                    case 0:
                        BenchNeighbor.getNeighborsSorted(graphDb,
                            modGet(warmupNhbrs, i));
                        break;
                    case 1:
                        NeighborNodeBench.getNeighborNode(graphDb,
                            modGet(warmupNhbrNodeIds, i),
                            modGet(warmupNhbrNodeAttrIds, i),
                            modGet(warmupNhbrNodeAttrs, i));
                        break;
                    case 2:
                        BenchNode.getNodes(graphDb, label,
                            modGet(warmupNodeAttrIds1, i),
                            modGet(warmupNodeAttrs1, i));
                        break;
                    case 3:
                        BenchNeighborAtype.getNeighborsSorted(graphDb,
                            modGet(warmupNhbrAtypeIds, i),
                            BenchNeighborAtype.atypeMap[modGet(
                                warmupNhbrAtypeAtypes, i).intValue()]);
                        break;
                    case 4:
                        BenchNode.getNodes(graphDb, label,
                            modGet(warmupNodeAttrIds1, i),
                            modGet(warmupNodeAttrs1, i),
                            modGet(warmupNodeAttrIds2, i),
                            modGet(warmupNodeAttrs2, i));
                        break;
                }
            }

            rand.setSeed(1618L); // re-seed

            // measure
            System.out.printf("Measure %d queries\n", MEASURE_N);
            long start, end;
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                randQuery = rand.nextInt(5);
                switch (randQuery) {
                    case 0:
                        // get_nhbrs(n)
                        start = System.nanoTime();
                        List<Long> nodes = BenchNeighbor.getNeighborsSorted(
                            graphDb, modGet(nhbrs, i));
                        end = System.nanoTime();
                        nhbrOut.println(
                            nodes.size() + "," + (end - start) / 1e3);
                        break;
                    case 1:
                        // get_nhbrs(n, attr)
                        start = System.nanoTime();
                        nodes = NeighborNodeBench.getNeighborNode(graphDb,
                            modGet(nhbrNodeIds, i),
                            modGet(nhbrNodeAttrIds, i),
                            modGet(nhbrNodeAttrs, i));
                        end = System.nanoTime();
                        nhbrNodeOut.println(
                            nodes.size() + "," + (end - start) / 1e3);
                        break;
                    case 2:
                        // get_nodes(attr)
                        start = System.nanoTime();
                        Set<Long> nodeSet = BenchNode.getNodes(graphDb, label,
                            modGet(nodeAttrIds1, i),
                            modGet(nodeAttrs1, i));
                        end = System.nanoTime();
                        nodeOut.println(
                            nodeSet.size() + "," + (end - start) / 1e3);
                        break;
                    case 3:
                        // get_nhbrs(n, atype)
                        start = System.nanoTime();
                        nodes = BenchNeighborAtype.getNeighborsSorted(graphDb,
                            modGet(nhbrAtypeIds, i),
                            BenchNeighborAtype.atypeMap[modGet(
                                nhbrAtypeAtypes, i).intValue()]);
                        end = System.nanoTime();
                        nhbrAtypeOut.println(
                            nodes.size() + "," + (end - start) / 1e3);
                        break;
                    case 4:
                    // get_nodes(attr1, attr2)
                        start = System.nanoTime();
                        nodeSet = BenchNode.getNodes(graphDb, label,
                            modGet(nodeAttrIds1, i),
                            modGet(nodeAttrs1, i),
                            modGet(nodeAttrIds2, i),
                            modGet(nodeAttrs2, i));
                        end = System.nanoTime();
                        doubleNodeOut.println(
                            nodeSet.size() + "," + (end - start) / 1e3);
                        break;
                }
            }

            nhbrOut.close();
            nhbrNodeOut.close();
            nhbrAtypeOut.close();
            nodeOut.close();
            doubleNodeOut.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            tx.success();
            tx.close();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

}
