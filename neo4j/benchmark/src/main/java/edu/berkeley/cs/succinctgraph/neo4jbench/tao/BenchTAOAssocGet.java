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
import java.util.Set;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.modGet;

public class BenchTAOAssocGet {

    final static TAOImpls taoImpls = new TAOImpls();

    static int numWarmupQueries, numMeasureQueries;
    static List<Long> warmupAssocGetNodes, assocGetNodes;
    static List<Long> warmupAssocGetAtypes, assocGetAtypes;
    static List<Set<Long>> warmupAssocGetDstIdSets, assocGetDstIdSets;
    static List<Long> warmupAssocGetTimeLows, assocGetTimeLows;
    static List<Long> warmupAssocGetTimeHighs, assocGetTimeHighs;

    public static void main(String[] args) {
        String type = args[0];
        String dbPath = args[1];
        String warmupQueryFile = args[2];
        String queryFile = args[3];
        String outputFile = args[4];
        numWarmupQueries = Integer.parseInt(args[5]);
        numMeasureQueries = Integer.parseInt(args[6]);

        warmupAssocGetNodes = new ArrayList<>();
        assocGetNodes = new ArrayList<>();
        warmupAssocGetAtypes = new ArrayList<>();
        assocGetAtypes = new ArrayList<>();
        warmupAssocGetDstIdSets = new ArrayList<>();
        assocGetDstIdSets = new ArrayList<>();
        warmupAssocGetTimeLows = new ArrayList<>();
        assocGetTimeLows = new ArrayList<>();
        warmupAssocGetTimeHighs = new ArrayList<>();
        assocGetTimeHighs = new ArrayList<>();

        String neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length >= 8) {
            neo4jPageCacheMemory = args[7];
        }

        BenchUtils.readAssocGetQueries(
            warmupQueryFile, warmupAssocGetNodes, warmupAssocGetAtypes,
            warmupAssocGetDstIdSets, warmupAssocGetTimeLows,
            warmupAssocGetTimeHighs);

        BenchUtils.readAssocGetQueries(
            queryFile, assocGetNodes, assocGetAtypes,
            assocGetDstIdSets, assocGetTimeLows, assocGetTimeHighs);

        if (type.equals("latency")) {
            benchAssocGetLatency(dbPath, neo4jPageCacheMemory, outputFile);
        } else {
            System.err.println("Unknown type: " + type);
        }
    }

    private static void benchAssocGetLatency(
        String dbPath, String neo4jPageCacheMem, String outputFile) {

        System.out.println("Benchmarking assoc_get() queries");
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
                taoImpls.assocGet(db,
                    modGet(warmupAssocGetNodes, i),
                    modGet(warmupAssocGetAtypes, i),
                    modGet(warmupAssocGetDstIdSets, i),
                    modGet(warmupAssocGetTimeLows, i),
                    modGet(warmupAssocGetTimeHighs, i));
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
                List<Assoc> assocs = taoImpls.assocGet(db,
                    modGet(assocGetNodes, i),
                    modGet(assocGetAtypes, i),
                    modGet(assocGetDstIdSets, i),
                    modGet(assocGetTimeLows, i),
                    modGet(assocGetTimeHighs, i));
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
