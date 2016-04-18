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

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;
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
        int numClients = Integer.parseInt(args[7]);
        boolean tuned = Boolean.valueOf(args[8]);
        String neo4jPageCacheMemory = args[9];

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
        } if (type.equals("throughput")) {
            benchAssocTimeRangeThroughput(tuned, dbPath, neo4jPageCacheMemory, numClients);
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

    static class RunAssocTimeRangeThroughput implements Runnable {
        private int clientId;
        private List<Long> warmupNodes, nodes;
        private List<Long> warmupAtypes, atypes;
        private List<Long> warmupTimeHighs, timeHighs;
        private List<Long> warmupTimeLows, timeLows;
        private List<Integer> warmupLimits, limits;
        private GraphDatabaseService graphDb;

        public RunAssocTimeRangeThroughput(
          int clientId, List<Long> warmupNodes, List<Long> nodes,
          List<Long> warmupAtypes, List<Long> atypes,
          List<Long> warmupTimeHighs, List<Long> timeHighs,
          List<Long> warmupTimeLows, List<Long> timeLows,
          List<Integer> warmupLimits, List<Integer> limits,
          GraphDatabaseService graphDb) {

            this.clientId = clientId;
            this.warmupNodes = warmupNodes;
            this.nodes = nodes;
            this.warmupAtypes = warmupAtypes;
            this.atypes = atypes;
            this.warmupTimeHighs = warmupTimeHighs;
            this.timeHighs = timeHighs;
            this.warmupTimeLows = warmupTimeLows;
            this.timeLows = timeLows;
            this.warmupLimits = warmupLimits;
            this.limits = limits;
            this.graphDb = graphDb;
        }

        public void run() {
            Transaction tx = graphDb.beginTx();
            PrintWriter out = null;
            Random rand = new Random(1618 + clientId);
            try {
                // true for append
                out = new PrintWriter(new BufferedWriter(
                  new FileWriter("neo4j_throughput_assoc_time_range.txt", true)));

                // warmup
                int i = 0, queryIdx = 0;
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(warmupNodes.size());
                    taoImpls.assocTimeRange(graphDb,
                      modGet(warmupNodes, queryIdx), modGet(warmupAtypes, queryIdx),
                      modGet(warmupTimeLows, queryIdx), modGet(warmupTimeHighs, queryIdx),
                      modGet(warmupLimits, queryIdx));
                    ++i;
                }

                // measure
                i = 0;
                long edges = 0;
                int querySize = nodes.size();
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(querySize);
                    List<Assoc> neighbors = taoImpls.assocTimeRange(graphDb,
                      modGet(nodes, queryIdx), modGet(atypes, queryIdx),
                      modGet(timeLows, queryIdx), modGet(timeHighs, queryIdx),
                      modGet(limits, queryIdx));
                    edges += neighbors.size();
                    ++i;
                }
                long end = System.nanoTime();
                double totalSeconds = (end - start) * 1. / 1e9;
                double queryThput = ((double) i) / totalSeconds;
                double edgesThput = ((double) edges) / totalSeconds;

                // cooldown
                long cooldownStart = System.nanoTime();
                while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                    queryIdx = rand.nextInt(querySize);
                    taoImpls.assocTimeRange(graphDb,
                      modGet(nodes, queryIdx), modGet(atypes, queryIdx),
                      modGet(timeLows, queryIdx), modGet(timeHighs, queryIdx),
                      modGet(limits, queryIdx));
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

    private static void benchAssocTimeRangeThroughput(boolean tuned, String dbPath,
      String neo4jPageCacheMem, int numClients) {

        GraphDatabaseService graphDb;
        System.out.println("About to open database");
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
                clients.add(new Thread(new RunAssocTimeRangeThroughput(
                  i, warmupAssocTimeRangeNodes, assocTimeRangeNodes,
                  warmupAssocTimeRangeAtypes, assocTimeRangeAtypes,
                  warmupAssocTimeRangeTimeHighs, assocTimeRangeTimeHighs,
                  warmupAssocTimeRangeTimeLows, assocTimeRangeTimeLows,
                  warmupAssocTimeRangeLimits, assocTimeRangeLimits,
                  graphDb)));
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
