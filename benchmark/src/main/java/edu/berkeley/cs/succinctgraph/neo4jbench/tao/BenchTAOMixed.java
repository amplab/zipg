package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils;
import org.neo4j.graphdb.GraphDatabaseService;
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

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.modGet;

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
        String neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length > 19) {
            neo4jPageCacheMemory = args[19];
        }

        // TODO: unif. random for now -- change to TAO paper distribution later

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
            mixLatency(dbPath, neo4jPageCacheMemory,
                assocRangeOut, assocCountOut, objGetOut,
                assocGetOut, assocTimeRangeOut);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    /**
     * Note: the mixing order can affect query performance.
     */
    private static void mixLatency(
        String DB_PATH, String neo4jPageCacheMem,
        String assocRangeOut, String assocCountOut,
        String objGetOut, String assocGetOut, String assocTimeRangeOut) {

        // START SNIPPET: startDb
        GraphDatabaseService db = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(DB_PATH)
            .setConfig(GraphDatabaseSettings.cache_type, "none")
            .setConfig(
                GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
            .newGraphDatabase();

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

            BenchUtils.fullWarmup(db);
            BenchUtils.awaitIndexes(db);

            long seed = 1618L;
            Random rand = new Random(seed);
            int randQuery;

            // warmup
            System.out.println("Warming up queries");
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
            System.out.println("Measure queries");
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
}
