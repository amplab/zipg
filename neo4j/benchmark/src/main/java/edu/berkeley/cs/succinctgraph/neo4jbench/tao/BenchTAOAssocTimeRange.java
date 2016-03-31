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

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.modGet;

public class BenchTAOAssocTimeRange {

    final static TAOImpls taoImpls = new TAOImpls();

    static int numWarmupQueries, numMeasureQueries;
    static List<Long> warmupAssocTimeRangeNodes, assocTimeRangeNodes;
    static List<Long> warmupAssocTimeRangeAtypes, assocTimeRangeAtypes;
    static List<Long> warmupAssocTimeRangeTimeLows, assocTimeRangeTimeLows;
    static List<Long> warmupAssocTimeRangeTimeHighs, assocTimeRangeTimeHighs;
    static List<Integer> warmupAssocTimeRangeLimits, assocTimeRangeLimits;

    public static void main(String[] args) {
        String type = args[0];
        String dbPath = args[1];
        String warmupQueryFile = args[2];
        String queryFile = args[3];
        String outputFile = args[4];
        numWarmupQueries = Integer.parseInt(args[5]);
        numMeasureQueries = Integer.parseInt(args[6]);

        warmupAssocTimeRangeNodes = new ArrayList<>();
        assocTimeRangeNodes = new ArrayList<>();
        warmupAssocTimeRangeAtypes = new ArrayList<>();
        assocTimeRangeAtypes = new ArrayList<>();
        warmupAssocTimeRangeTimeLows = new ArrayList<>();
        assocTimeRangeTimeLows = new ArrayList<>();
        warmupAssocTimeRangeTimeHighs = new ArrayList<>();
        assocTimeRangeTimeHighs = new ArrayList<>();
        warmupAssocTimeRangeLimits = new ArrayList<>();
        assocTimeRangeLimits = new ArrayList<>();

        String neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length >= 8) {
            neo4jPageCacheMemory = args[7];
        }

        BenchUtils.readAssocTimeRangeQueries(
            warmupQueryFile, warmupAssocTimeRangeNodes,
            warmupAssocTimeRangeAtypes, warmupAssocTimeRangeTimeLows,
            warmupAssocTimeRangeTimeHighs, warmupAssocTimeRangeLimits);

        BenchUtils.readAssocTimeRangeQueries(
            queryFile, assocTimeRangeNodes, assocTimeRangeAtypes,
            assocTimeRangeTimeLows, assocTimeRangeTimeHighs,
            assocTimeRangeLimits);

        if (type.equals("latency")) {
            benchAssocTimeRangeLatency(
                dbPath, neo4jPageCacheMemory, outputFile);
        } else {
            System.err.println("Unknown type: " + type);
        }
    }

    private static void benchAssocTimeRangeLatency(
        String dbPath, String neo4jPageCacheMem, String outputFile) {

        System.out.println("Benchmarking assoc_time_range() queries");
        System.out.println("Setting Neo4j's dbms.pagecache.memory: " +
            neo4jPageCacheMem);

        GraphDatabaseService db = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(dbPath)
            .setConfig(GraphDatabaseSettings.cache_type, "none")
            .setConfig(
                GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
            .newGraphDatabase();

        BenchUtils.registerShutdownHook(db);
        Transaction tx = db.beginTx();
        try {
            BenchUtils.fullWarmup(db);

            PrintWriter out = new PrintWriter(new BufferedWriter(
                new FileWriter(outputFile)));
            PrintWriter resOut = null;
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = new PrintWriter(new BufferedWriter(
                    new FileWriter(outputFile + ".neo4j_result")));
            }

            System.out.println(
                "Warming up for " + numWarmupQueries + " queries");
            for (int i = 0; i < numWarmupQueries; ++i) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                taoImpls.assocTimeRange(db,
                    modGet(warmupAssocTimeRangeNodes, i),
                    modGet(warmupAssocTimeRangeAtypes, i),
                    modGet(warmupAssocTimeRangeTimeLows, i),
                    modGet(warmupAssocTimeRangeTimeHighs, i),
                    modGet(warmupAssocTimeRangeLimits, i));
            }

            System.out.println(
                "Measuring for " + numMeasureQueries + " queries");
            for (int i = 0; i < numMeasureQueries; ++i) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                long queryStart = System.nanoTime();
                List<Assoc> assocs = taoImpls.assocTimeRange(db,
                    modGet(assocTimeRangeNodes, i),
                    modGet(assocTimeRangeAtypes, i),
                    modGet(assocTimeRangeTimeLows, i),
                    modGet(assocTimeRangeTimeHighs, i),
                    modGet(assocTimeRangeLimits, i));
                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(assocs.size() + "," + microsecs);

                if (resOut != null) {
                    for (Assoc assoc : assocs) {
                        resOut.print(assoc.toString());
                        resOut.print(" ");
                    }
                    resOut.println();
                }
            }
            out.close();
            if (resOut != null) {
                resOut.flush();
                resOut.close();
            }

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
