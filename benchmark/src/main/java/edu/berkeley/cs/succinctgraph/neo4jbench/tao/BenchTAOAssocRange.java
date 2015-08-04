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

public class BenchTAOAssocRange {

    static TAOImpls taoImpls = new TAOImpls();

    static int numWarmupQueries, numMeasureQueries;
    static List<Long> warmupAssocRangeNodes, assocRangeNodes;
    static List<Long> warmupAssocRangeAtypes, assocRangeAtypes;
    static List<Integer> warmupAssocRangeOffsets, assocRangeOffsets;
    static List<Integer> warmupAssocRangeLengths, assocRangeLengths;

    public static void main(String[] args) {
        String type = args[0];
        String dbPath = args[1];
        String warmupQueryFile = args[2];
        String queryFile = args[3];
        String outputFile = args[4];
        numWarmupQueries = Integer.parseInt(args[5]);
        numMeasureQueries = Integer.parseInt(args[6]);

        warmupAssocRangeNodes = new ArrayList<>();
        assocRangeNodes = new ArrayList<>();
        warmupAssocRangeAtypes = new ArrayList<>();
        assocRangeAtypes = new ArrayList<>();
        warmupAssocRangeOffsets = new ArrayList<>();
        assocRangeOffsets = new ArrayList<>();
        warmupAssocRangeLengths = new ArrayList<>();
        assocRangeLengths = new ArrayList<>();

        String neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length >= 8) {
            neo4jPageCacheMemory = args[7];
        }

        BenchUtils.readAssocRangeQueries(
            warmupQueryFile, warmupAssocRangeNodes, warmupAssocRangeAtypes,
            warmupAssocRangeOffsets, warmupAssocRangeLengths);

        BenchUtils.readAssocRangeQueries(
            queryFile, assocRangeNodes, assocRangeAtypes,
            assocRangeOffsets, assocRangeLengths);

        if (type.equals("latency")) {
            benchAssocRangeLatency(dbPath, neo4jPageCacheMemory, outputFile);
        } else {
            System.err.println("Unknown type: " + type);
        }
    }

    private static void benchAssocRangeLatency(
        String dbPath, String neo4jPageCacheMem, String outputFile) {

        System.out.println("Benchmarking assoc_range() queries");
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

            System.out.println(
                "Warming up for " + numWarmupQueries + " queries");
            for (int i = 0; i < numWarmupQueries; ++i) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
                taoImpls.assocRange(db,
                    modGet(warmupAssocRangeNodes, i),
                    modGet(warmupAssocRangeAtypes, i),
                    modGet(warmupAssocRangeOffsets, i),
                    modGet(warmupAssocRangeLengths, i));
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
                List<Assoc> assocs = taoImpls.assocRange(db,
                    modGet(assocRangeNodes, i),
                    modGet(assocRangeAtypes, i),
                    modGet(assocRangeOffsets, i),
                    modGet(assocRangeLengths, i));
                long queryEnd = System.nanoTime();
                assert(assocs.size() <= modGet(assocRangeLengths, i));
                assert(!assocs.isEmpty()); // due to our query generation scheme
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(assocs.size() + "," + microsecs);
            }
            out.close();

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
