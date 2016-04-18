package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.modGet;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cache_type;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;

public class BenchTAOMixed {

    final static TAOImpls taoImpls = new TAOImpls();

    private static int WARMUP_N;
    private static int MEASURE_N;

    // assoc_range()
    static List<Long> warmupAssocRangeNodes = new ArrayList<>();
    static List<Long> assocRangeNodes = new ArrayList<>();
    static List<Long> warmupAssocRangeAtypes = new ArrayList<>();
    static List<Long> assocRangeAtypes = new ArrayList<>();
    static List<Integer> warmupAssocRangeOffsets = new ArrayList<>();
    static List<Integer> assocRangeOffsets = new ArrayList<>();
    static List<Integer> warmupAssocRangeLengths = new ArrayList<>();
    static List<Integer> assocRangeLengths = new ArrayList<>();

    // assoc_count()
    static List<Long> warmupAssocCountNodes = new ArrayList<>();
    static List<Long> assocCountNodes = new ArrayList<>();
    static List<Long> warmupAssocCountAtypes = new ArrayList<>();
    static List<Long> assocCountAtypes = new ArrayList<>();

    // obj_get()
    static List<Long> warmupObjGetIds = new ArrayList<>();
    static List<Long> objGetIds = new ArrayList<>();

    // assoc_get()
    static List<Long> warmupAssocGetNodes = new ArrayList<>();
    static List<Long> assocGetNodes = new ArrayList<>();
    static List<Long> warmupAssocGetAtypes = new ArrayList<>();
    static List<Long> assocGetAtypes = new ArrayList<>();
    static List<Set<Long>> warmupAssocGetDstIdSets = new ArrayList<>();
    static List<Set<Long>> assocGetDstIdSets = new ArrayList<>();
    static List<Long> warmupAssocGetTimeLows = new ArrayList<>();
    static List<Long> assocGetTimeLows = new ArrayList<>();
    static List<Long> warmupAssocGetTimeHighs = new ArrayList<>();
    static List<Long> assocGetTimeHighs = new ArrayList<>();

    // assoc_time_range()
    static List<Long> warmupAssocTimeRangeNodes = new ArrayList<>();
    static List<Long> assocTimeRangeNodes = new ArrayList<>();
    static List<Long> warmupAssocTimeRangeAtypes = new ArrayList<>();
    static List<Long> assocTimeRangeAtypes = new ArrayList<>();
    static List<Long> warmupAssocTimeRangeTimeLows = new ArrayList<>();
    static List<Long> assocTimeRangeTimeLows = new ArrayList<>();
    static List<Long> warmupAssocTimeRangeTimeHighs = new ArrayList<>();
    static List<Long> assocTimeRangeTimeHighs = new ArrayList<>();
    static List<Integer> warmupAssocTimeRangeLimits = new ArrayList<>();
    static List<Integer> assocTimeRangeLimits = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        String type = args[0];
        String dbPath = args[1];
        String warmupAssocRangeFile = args[2];
        String queryAssocRangeFile = args[3];
        String warmupAssocCountFile = args[4];
        String queryAssocCountFile = args[5];
        String warmupObjGetFile = args[6];
        String queryObjGetFile = args[7];
        String warmupAssocGetFile = args[8];
        String queryAssocGetFile = args[9];
        String warmupAssocTimeRangeFile = args[10];
        String queryAssocTimeRangeFile = args[11];

        String assocRangeOut = args[12];
        String assocCountOut = args[13];
        String objGetOut = args[14];
        String assocGetOut = args[15];
        String assocTimeRangeOut = args[16];

        WARMUP_N = Integer.parseInt(args[17]);
        MEASURE_N = Integer.parseInt(args[18]);
        int numClients = Integer.parseInt(args[19]);
        boolean tuned = Boolean.valueOf(args[20]);

        // Use ints for first two, to avoid expensive long nonnegative mod
        // All practical graphs have less than 2^31 nodes, anyway
        int numNodes = Integer.parseInt(args[21]);
        int numAtypes = Integer.parseInt(args[22]);
        long minTime = Long.parseLong(args[23]);
        long maxTime = Long.parseLong((args[24]));
        String neo4jPageCacheMemory = args[25];

        // assoc_range()
        BenchUtils.readAssocRangeQueries(
            warmupAssocRangeFile, warmupAssocRangeNodes, warmupAssocRangeAtypes,
            warmupAssocRangeOffsets, warmupAssocRangeLengths);

        BenchUtils.readAssocRangeQueries(
            queryAssocRangeFile, assocRangeNodes, assocRangeAtypes,
            assocRangeOffsets, assocRangeLengths);

        // assoc_count()
        BenchUtils.getNeighborAtypeQueries(
            warmupAssocCountFile,
            warmupAssocCountNodes, warmupAssocCountAtypes);

        BenchUtils.getNeighborAtypeQueries(
            queryAssocCountFile, assocCountNodes, assocCountAtypes);

        // obj_get()
        BenchUtils.getNeighborQueries(warmupObjGetFile, warmupObjGetIds);
        BenchUtils.getNeighborQueries(queryObjGetFile, objGetIds);

        // assoc_get()
        BenchUtils.readAssocGetQueries(
            warmupAssocGetFile, warmupAssocGetNodes, warmupAssocGetAtypes,
            warmupAssocGetDstIdSets, warmupAssocGetTimeLows,
            warmupAssocGetTimeHighs);

        BenchUtils.readAssocGetQueries(
            queryAssocGetFile, assocGetNodes, assocGetAtypes,
            assocGetDstIdSets, assocGetTimeLows, assocGetTimeHighs);

        // assoc_time_range()
        BenchUtils.readAssocTimeRangeQueries(
            warmupAssocTimeRangeFile, warmupAssocTimeRangeNodes,
            warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows,
            warmupAssocTimeRangeTimeHighs, warmupAssocTimeRangeLimits);

        BenchUtils.readAssocTimeRangeQueries(
            queryAssocTimeRangeFile, assocTimeRangeNodes, assocTimeRangeAtypes,
            assocTimeRangeTimeLows, assocTimeRangeTimeHighs,
            assocTimeRangeLimits);

        // start!

        if (type.equals("latency")) {
            mixLatency(tuned, dbPath, neo4jPageCacheMemory,
                assocRangeOut, assocCountOut, objGetOut,
                assocGetOut, assocTimeRangeOut);
        } else if (type.equals("throughput")) {
            throughput(tuned, dbPath, neo4jPageCacheMemory, numClients,
                numNodes, numAtypes, minTime, maxTime);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    /**
     * Note: the mixing order can affect query performance.
     */
    private static void mixLatency(boolean tuned,
        String DB_PATH, String neo4jPageCacheMem,
        String assocRangeOut, String assocCountOut,
        String objGetOut, String assocGetOut, String assocTimeRangeOut) {

        // START SNIPPET: startDb
        GraphDatabaseService db;
        if (tuned) {
            db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(DB_PATH)
                .setConfig(cache_type, "none")
                .setConfig(
                    pagecache_memory, neo4jPageCacheMem)
                .newGraphDatabase();
        } else {
            db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        }

        BenchUtils.registerShutdownHook(db);
        Transaction tx = db.beginTx();
        try {
            PrintWriter assocRangePw = new PrintWriter(new BufferedWriter(
                new FileWriter(assocRangeOut)));
            PrintWriter assocCountPw = new PrintWriter(new BufferedWriter(
                new FileWriter(assocCountOut)));
            PrintWriter objGetPw = new PrintWriter(new BufferedWriter(
                new FileWriter(objGetOut)));
            PrintWriter assocGetPw = new PrintWriter(new BufferedWriter(
                new FileWriter(assocGetOut)));
            PrintWriter assocTimeRangePw = new PrintWriter(new BufferedWriter(
                new FileWriter(assocTimeRangeOut)));

            if (tuned) {
                BenchUtils.fullWarmup(db);
            }

            long seed = 1618L;
            Random rand = new Random(seed);
            int randQuery;

            // warmup
            System.out.printf("Warming up %d queries\n", WARMUP_N);
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                randQuery = rand.nextInt(5);
                switch (randQuery) {
                    case 0:
                        taoImpls.assocRange(db,
                            modGet(warmupAssocRangeNodes, i),
                            modGet(warmupAssocRangeAtypes, i),
                            modGet(warmupAssocRangeOffsets, i),
                            modGet(warmupAssocRangeLengths, i));
                        break;
                    case 1:
                        taoImpls.assocCount(db,
                            modGet(warmupAssocCountNodes, i),
                            modGet(warmupAssocCountAtypes, i));
                        break;
                    case 2:
                        taoImpls.objGet(db, modGet(warmupObjGetIds, i));
                        break;
                    case 3:
                        taoImpls.assocGet(db,
                            modGet(warmupAssocGetNodes, i),
                            modGet(warmupAssocGetAtypes, i),
                            modGet(warmupAssocGetDstIdSets, i),
                            modGet(warmupAssocGetTimeLows, i),
                            modGet(warmupAssocGetTimeHighs, i));
                        break;
                    case 4:
                        taoImpls.assocTimeRange(db,
                            modGet(warmupAssocTimeRangeNodes, i),
                            modGet(warmupAssocTimeRangeAtypes, i),
                            modGet(warmupAssocTimeRangeTimeLows, i),
                            modGet(warmupAssocTimeRangeTimeHighs, i),
                            modGet(warmupAssocTimeRangeLimits, i));
                        break;
                }
            }

            rand.setSeed(1618L); // re-seed

            // measure
            System.out.printf("Measure %d queries\n", MEASURE_N);
            long start, end, cnt;
            List<String> attrs;
            List<Assoc> assocs;
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                randQuery = rand.nextInt(5);
                switch (randQuery) {
                    case 0:
                        // assoc_range
                        start = System.nanoTime();
                        assocs = taoImpls.assocRange(db,
                            modGet(assocRangeNodes, i),
                            modGet(assocRangeAtypes, i),
                            modGet(assocRangeOffsets, i),
                            modGet(assocRangeLengths, i));
                        end = System.nanoTime();
                        assocRangePw.println(
                            assocs.size() + "," + (end - start) / 1e3);
                        break;
                    case 1:
                        // assoc_count
                        start = System.nanoTime();
                        cnt = taoImpls.assocCount(db,
                            modGet(assocCountNodes, i),
                            modGet(assocCountAtypes, i));
                        end = System.nanoTime();
                        assocCountPw.println(
                            cnt + "," + (end - start) / 1e3);
                        break;
                    case 2:
                        // obj_get
                        start = System.nanoTime();
                        attrs = taoImpls.objGet(db, modGet(objGetIds, i));
                        end = System.nanoTime();
                        objGetPw.println(
                            attrs.size() + "," + (end - start) / 1e3);
                        break;
                    case 3:
                        // assoc_get
                        start = System.nanoTime();
                        assocs = taoImpls.assocGet(db,
                            modGet(assocGetNodes, i),
                            modGet(assocGetAtypes, i),
                            modGet(assocGetDstIdSets, i),
                            modGet(assocGetTimeLows, i),
                            modGet(assocGetTimeHighs, i));
                        end = System.nanoTime();
                        assocGetPw.println(
                            assocs.size() + "," + (end - start) / 1e3);
                        break;
                    case 4:
                        // assoc_time_range
                        start = System.nanoTime();
                        assocs = taoImpls.assocTimeRange(db,
                            modGet(assocTimeRangeNodes, i),
                            modGet(assocTimeRangeAtypes, i),
                            modGet(assocTimeRangeTimeLows, i),
                            modGet(assocTimeRangeTimeHighs, i),
                            modGet(assocTimeRangeLimits, i));
                        end = System.nanoTime();
                        assocTimeRangePw.println(
                            assocs.size() + "," + (end - start) / 1e3);
                        break;
                }
            }

            assocRangePw.close();
            assocCountPw.close();
            objGetPw.close();
            assocGetPw.close();
            assocTimeRangePw.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            tx.success();
            tx.close();
            System.out.println("Shutting down database ...");
            db.shutdown();
        }
    }

    static int dispatchMixQueryWarmup(GraphDatabaseService db,
        int randQuery, Random rand, int warmupAssocRangeSize,
        int warmupAssocCountSize, int objGetSize, int warmupAssocGetSize,
        int warmupAssocTimeRangeSize) {

        int i;
        switch (randQuery) {
            case 0:
                // assoc_range
                i = rand.nextInt(warmupAssocRangeSize);
                return taoImpls.assocRange(db,
                    modGet(warmupAssocRangeNodes, i),
                    modGet(warmupAssocRangeAtypes, i),
                    modGet(warmupAssocRangeOffsets, i),
                    modGet(warmupAssocRangeLengths, i)).size();
            case 1:
                // obj_get
                i = rand.nextInt(objGetSize);
                taoImpls.objGet(db, modGet(objGetIds, i));
                break;
            case 2:
                // assoc_get
                i = rand.nextInt(warmupAssocGetSize);
                return taoImpls.assocGet(db,
                    modGet(warmupAssocGetNodes, i),
                    modGet(warmupAssocGetAtypes, i),
                    modGet(warmupAssocGetDstIdSets, i),
                    modGet(warmupAssocGetTimeLows, i),
                    modGet(warmupAssocGetTimeHighs, i)).size();
            case 3:
                // assoc_count
                i = rand.nextInt(warmupAssocCountSize);
                taoImpls.assocCount(db,
                    modGet(warmupAssocCountNodes, i),
                    modGet(warmupAssocCountAtypes, i));
                break;
            case 4:
                // assoc_time_range
                i = rand.nextInt(warmupAssocTimeRangeSize);
                return taoImpls.assocTimeRange(db,
                    modGet(warmupAssocTimeRangeNodes, i),
                    modGet(warmupAssocTimeRangeAtypes, i),
                    modGet(warmupAssocTimeRangeTimeLows, i),
                    modGet(warmupAssocTimeRangeTimeHighs, i),
                    modGet(warmupAssocTimeRangeLimits, i)).size();
        }
        return 0;
    }

    static int dispatchMixQuery(GraphDatabaseService db,
        int randQuery, Random rand, int assocRangeSize, int assocCountSize,
        int objGetSize, int assocGetSize, int assocTimeRangeSize) {

        int i;
        switch (randQuery) {
            case 0:
                // assoc_range
                i = rand.nextInt(assocRangeSize);
                return taoImpls.assocRange(db,
                    modGet(assocRangeNodes, i),
                    modGet(assocRangeAtypes, i),
                    modGet(assocRangeOffsets, i),
                    modGet(assocRangeLengths, i)).size();
            case 1:
                // obj_get
                i = rand.nextInt(objGetSize);
                taoImpls.objGet(db, modGet(objGetIds, i));
                break;
            case 2:
                // assoc_get
                i = rand.nextInt(assocGetSize);
                return taoImpls.assocGet(db,
                    modGet(assocGetNodes, i),
                    modGet(assocGetAtypes, i),
                    modGet(assocGetDstIdSets, i),
                    modGet(assocGetTimeLows, i),
                    modGet(assocGetTimeHighs, i)).size();
            case 3:
                // assoc_count
                i = rand.nextInt(assocCountSize);
                taoImpls.assocCount(db,
                    modGet(assocCountNodes, i),
                    modGet(assocCountAtypes, i));
                break;
            case 4:
                // assoc_time_range
                i = rand.nextInt(assocTimeRangeSize);
                return taoImpls.assocTimeRange(db,
                    modGet(assocTimeRangeNodes, i),
                    modGet(assocTimeRangeAtypes, i),
                    modGet(assocTimeRangeTimeLows, i),
                    modGet(assocTimeRangeTimeHighs, i),
                    modGet(assocTimeRangeLimits, i)).size();
        }
        return 0;
    }

    static class RunTAOMixThroughput implements Runnable {
        private int clientId;
        private GraphDatabaseService graphDb;

        int warmupAssocRangeSize = warmupAssocRangeNodes.size();
        int warmupAssocCountSize = warmupAssocCountNodes.size();
        int warmupObjGetSize = warmupObjGetIds.size();
        int warmupAssocGetSize = warmupAssocGetNodes.size();
        int warmupAssocTimeRangeSize = warmupAssocTimeRangeNodes.size();

        int assocRangeSize = assocRangeNodes.size();
        int assocCountSize = assocCountNodes.size();
        int objGetSize = objGetIds.size();
        int assocGetSize = assocGetNodes.size();
        int assocTimeRangeSize = assocTimeRangeNodes.size();

        final Random rand;
        final int numNodes, numAtypes;
        final long minTime, maxTime;

        public RunTAOMixThroughput(int clientId, GraphDatabaseService graphDb,
            int numNodes, int numAtypes, long minTime, long maxTime) {

            this.clientId = clientId;
            this.graphDb = graphDb;
            this.rand = new Random(1618 + clientId);
            this.numNodes = numNodes;
            this.numAtypes = numAtypes;
            this.minTime = minTime;
            this.maxTime = maxTime;
        }

        private long nonNegativeMod(long x, long mod) {
            long res = x % mod;
            return res + ((res < 0) ? mod : 0);
        }

        private int dispatchQuery(
            GraphDatabaseService db, int randQuery, boolean warmup) {

            int i;
            switch (randQuery) {
                case 0:
                    // assoc_range
                    return taoImpls.assocRange(db,
                        rand.nextInt(numNodes),
                        rand.nextInt(numAtypes),
                        0,
                        10000 // from LinkBench
                    ).size();
                case 1:
                    // obj_get
                    taoImpls.objGet(db, rand.nextInt(numNodes));
                    break;
                case 2:
                    // assoc_get
                    // FIXME: not sure what to do with dstIdSet
                    if (warmup) {
                        i = rand.nextInt(warmupAssocGetSize);
                        return taoImpls.assocGet(db,
                            modGet(warmupAssocGetNodes, i),
                            modGet(warmupAssocGetAtypes, i),
                            modGet(warmupAssocGetDstIdSets, i),
                            modGet(warmupAssocGetTimeLows, i),
                            modGet(warmupAssocGetTimeHighs, i)).size();
                    }
                    i = rand.nextInt(assocGetSize);
                    return taoImpls.assocGet(db,
                        modGet(assocGetNodes, i),
                        modGet(assocGetAtypes, i),
                        modGet(assocGetDstIdSets, i),
                        modGet(assocGetTimeLows, i),
                        modGet(assocGetTimeHighs, i)).size();
                case 3:
                    // assoc_count
                    taoImpls.assocCount(db,
                        rand.nextInt(numNodes),
                        rand.nextInt(numAtypes));
                    break;
                case 4:
                    // assoc_time_range
                    return taoImpls.assocTimeRange(db,
                        rand.nextInt(numNodes),
                        rand.nextInt(numAtypes),
                        nonNegativeMod(rand.nextLong(), minTime),
                        nonNegativeMod(rand.nextLong(), maxTime),
                        10000 // from LinkBench
                    ).size();
            }
            return 0;
        }

        public void run() {
            Transaction tx = graphDb.beginTx();
            PrintWriter out = null;
            try {
                // true for append
                out = new PrintWriter(new BufferedWriter(
                    new FileWriter("neo4j_throughput_tao_mix.txt", true)));

                // warmup
                int i = 0;
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    // dispatchQuery(graphDb, TAOImpls.chooseQuery(rand), true);
                    dispatchMixQueryWarmup(graphDb, TAOImpls.chooseQuery(rand),
                        rand, warmupAssocRangeSize, warmupAssocCountSize,
                        warmupObjGetSize, warmupAssocGetSize,
                        warmupAssocTimeRangeSize);
                    ++i;
                }

                // measure
                i = 0;
                long edges = 0;
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
//                    edges += dispatchQuery(
//                        graphDb, TAOImpls.chooseQuery(rand), false);
                    edges += dispatchMixQuery(graphDb,
                        TAOImpls.chooseQuery(rand), rand,
                        assocRangeSize, assocCountSize, objGetSize,
                        assocGetSize, assocTimeRangeSize);
                    ++i;
                }
                long end = System.nanoTime();
                double totalSeconds = (end - start) * 1. / 1e9;
                double queryThput = ((double) i) / totalSeconds;
                double edgesThput = ((double) edges) / totalSeconds;

                // cooldown
                long cooldownStart = System.nanoTime();
                while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                    dispatchMixQuery(graphDb,
                        TAOImpls.chooseQuery(rand), rand,
                        assocRangeSize, assocCountSize, objGetSize,
                        assocGetSize, assocTimeRangeSize);
//                    dispatchQuery(
//                        graphDb, TAOImpls.chooseQuery(rand), false);
                    ++i;
                }
                out.printf("%.1f %.1f\n", queryThput, edgesThput);

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

    private static void throughput(boolean tuned, String dbPath,
        String neo4jPageCacheMem, int numClients,
        int numNodes, int numAtypes, long minTime, long maxTime) {

        GraphDatabaseService graphDb;
        System.out.println("About to open database");
        if (tuned) {
            graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbPath)
                .setConfig(cache_type, "none")
                .setConfig(
                    pagecache_memory, neo4jPageCacheMem)
                .newGraphDatabase();
        } else {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        }
        System.out.println("Done opening");
        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();
            if (tuned) {
                BenchUtils.fullWarmup(graphDb);
            }
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }
        try {
            List<Thread> clients = new ArrayList<>(numClients);
            for (int i = 0; i < numClients; ++i) {
                clients.add(new Thread(
                    new RunTAOMixThroughput(i, graphDb,
                        numNodes, numAtypes, minTime, maxTime)));
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
}
