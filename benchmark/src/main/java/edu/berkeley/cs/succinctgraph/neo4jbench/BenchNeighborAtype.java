package edu.berkeley.cs.succinctgraph.neo4jbench;

import edu.berkeley.cs.succinctgraph.neo4jbench.tao.AType;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.*;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;

public class BenchNeighborAtype {

    private static int WARMUP_N;
    private static int MEASURE_N;

    private static List<Long> warmupIds, queryIds;
    private static List<Long> warmupAtypes, queryAtypes;

    // assume 5 atypes
    public static RelationshipType[] atypeMap;
    static {
        atypeMap = new RelationshipType[5];
        // assume 5 atypes
        for (int i = 0; i < 5; ++i) {
            atypeMap[i] = new AType(String.valueOf(i));
        }
    }

    public static void main(String[] args) {
        String type = args[0];
        String dbPath = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        int numClients = Integer.parseInt(args[7]);
        boolean tuned = Boolean.valueOf(args[8]);
        String neo4jPageCacheMemory =
            GraphDatabaseSettings.pagecache_memory.getDefaultValue();
        if (args.length >= 10) {
            neo4jPageCacheMemory = args[9];
        }

        warmupIds = new ArrayList<>();
        queryIds = new ArrayList<>();
        warmupAtypes = new ArrayList<>();
        queryAtypes = new ArrayList<>();

        BenchUtils.getNeighborAtypeQueries(
            warmup_query_path, warmupIds, warmupAtypes);
        BenchUtils.getNeighborAtypeQueries(
            query_path, queryIds, queryAtypes);

        if (type.equals("latency")) {
            benchNeighborAtypeLatency(tuned,
                dbPath, neo4jPageCacheMemory, output_file);
        } else if (type.equals("throughput")) {
            benchThroughput(
                tuned, dbPath, neo4jPageCacheMemory, numClients, true);
        } else if (type.equals("edgeAttrs-throughput")) {
            benchThroughput(
                tuned, dbPath, neo4jPageCacheMemory, numClients, false);
        } else {
            throw new IllegalArgumentException("Unknown bench type: " + type);
        }
    }

    private static void benchThroughput(
        boolean tuned, String dbPath, String neo4jPageCacheMem, int numClients,
        boolean isNhbrAtype) {

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
                if (isNhbrAtype) {
                    clients.add(new Thread(new RunNeighborAtypeThroughput(
                        i, graphDb)));
                } else {
                    clients.add(new Thread(new RunEdgeAttrsThroughput(
                        i, graphDb)));
                }
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

    private static void benchNeighborAtypeLatency(
        boolean tuned, String dbPath, String neo4jPageCacheMem,
        String output_file) {

        List<Long> neighbors;

        System.out.println("Benchmarking getNeighborAtype queries");

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
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
                output_file)));
            PrintWriter resOut = null;
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = new PrintWriter(new BufferedWriter(
                    new FileWriter(output_file + ".neo4j_result")));
            }

            if (tuned) {
                BenchUtils.fullWarmup(graphDb);
            }
            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                getNeighborsSorted(
                    graphDb,
                    warmupIds.get(i % warmupIds.size()),
                    atypeMap[warmupAtypes.get(i % warmupAtypes.size())
                        .intValue()]);
            }

            System.out.println("Measuring for " + MEASURE_N + " queries");
            // measure
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                long queryStart = System.nanoTime();

                neighbors = getNeighborsSorted(
                    graphDb,
                    queryIds.get(i % queryIds.size()),
                    atypeMap[queryAtypes.get(i % queryAtypes.size())
                        .intValue()]);

                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(neighbors.size() + "," + microsecs);
            }

            tx.success();
            out.close();
            if (resOut != null) resOut.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            BenchUtils.printMemoryFootprint();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    static class RunNeighborAtypeThroughput implements Runnable {
        private int clientId;
        private GraphDatabaseService graphDb;

        public RunNeighborAtypeThroughput(
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
                    "neo4j_throughput_get_nhbrs_atype.txt", true)));

                // warmup
                int i = 0, queryIdx = 0, warmupSize = warmupIds.size();
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(warmupSize);
                    getNeighborsSorted(graphDb, modGet(warmupIds, queryIdx),
                        atypeMap[modGet(warmupAtypes, queryIdx).intValue()]);
                    ++i;
                }

                // measure
                i = 0;
                long edges = 0;
                int querySize = queryIds.size();
                List<Long> neighbors;
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(querySize);
                    neighbors = getNeighborsSorted(graphDb,
                        modGet(queryIds, queryIdx),
                        atypeMap[modGet(queryAtypes, queryIdx).intValue()]);
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
                    getNeighborsSorted(graphDb, modGet(warmupIds, i),
                        atypeMap[modGet(warmupAtypes, i).intValue()]);
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

    static class RunEdgeAttrsThroughput implements Runnable {
        private int clientId;
        private GraphDatabaseService graphDb;

        public RunEdgeAttrsThroughput(
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
                    "neo4j_throughput_getEdgeAttrs.txt", true)));

                // warmup
                int i = 0, queryIdx = 0, warmupSize = warmupIds.size();
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(warmupSize);
                    getEdgeAttrs(graphDb, modGet(warmupIds, queryIdx),
                        atypeMap[modGet(warmupAtypes, queryIdx).intValue()]);
                    ++i;
                }

                // measure
                i = 0;
                long edges = 0;
                int querySize = queryIds.size();
                List<String> edgeAttrs;
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(querySize);
                    edgeAttrs = getEdgeAttrs(graphDb,
                        modGet(queryIds, queryIdx),
                        atypeMap[modGet(queryAtypes, queryIdx).intValue()]);
                    edges += edgeAttrs.size();
                    ++i;
                }
                long end = System.nanoTime();
                double totalSeconds = (end - start) * 1. / 1e9;
                double queryThput = ((double) i) / totalSeconds;
                double edgesThput = ((double) edges) / totalSeconds;

                // cooldown
                long cooldownStart = System.nanoTime();
                while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                    getEdgeAttrs(graphDb,
                        modGet(queryIds, i),
                        atypeMap[modGet(queryAtypes, i).intValue()]);
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

//    public static List<Long> getNeighbors(
//        GraphDatabaseService graphDb, long id, RelationshipType relType) {
//
//        List<Long> neighbors = new ArrayList<>();
//        Node n = graphDb.getNodeById(id);
//        Iterable<Relationship> rels = n.getRelationships(
//            relType, Direction.OUTGOING);
//        for (Relationship r : rels) {
//            neighbors.add(r.getOtherNode(n).getId());
//        }
//        return neighbors;
//    }

    public static List<Long> getNeighborsSorted(
        GraphDatabaseService graphDb, long id, RelationshipType relType) {

        Node n = graphDb.getNodeById(id);
        Iterable<Relationship> rels = n.getRelationships(
            relType, Direction.OUTGOING);

        List<TimestampedId> timestampedNhbrs = new ArrayList<TimestampedId>();
        long timestamp, nhbrId;
        for (Relationship r : rels) {
            timestamp = (long) (r.getProperty("timestamp"));
            nhbrId = r.getOtherNode(n).getId();
            timestampedNhbrs.add(new TimestampedId(timestamp, nhbrId));
        }

        List<Long> neighbors = new ArrayList<Long>(timestampedNhbrs.size());
        Collections.sort(timestampedNhbrs);
        for (TimestampedId timestampedId : timestampedNhbrs) {
            neighbors.add(timestampedId.id);
        }
        return neighbors;
    }

    public static List<String> getEdgeAttrs(GraphDatabaseService graphDb,
                                          long id, RelationshipType relType) {
        Node n = graphDb.getNodeById(id);
        Iterable<Relationship> rels = n.getRelationships(
            relType, Direction.OUTGOING);

        List<TimestampedAttr> timestampedAttrs = new ArrayList<>();
        long timestamp;
        for (Relationship r : rels) {
            timestamp = (long) (r.getProperty("timestamp"));
            timestampedAttrs.add(new TimestampedAttr(
                timestamp, (String) r.getProperty("attr", "")));
        }

        List<String> attrs = new ArrayList<>(timestampedAttrs.size());
        Collections.sort(timestampedAttrs);

        for (TimestampedAttr timestampedAttr : timestampedAttrs) {
            attrs.add(timestampedAttr.attr);
        }
        return attrs;
    }

}
