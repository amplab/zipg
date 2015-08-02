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

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.TimestampedId;

public class BenchNeighborAtype {

    private static int WARMUP_N;
    private static int MEASURE_N;

    private static List<Long> warmupIds, queryIds;
    private static List<Integer> warmupAtypes, queryAtypes;

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
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        String neo4jPageCacheMemory =
            GraphDatabaseSettings.pagecache_memory.getDefaultValue();;
        if (args.length >= 8) {
            neo4jPageCacheMemory = args[7];
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
            benchNeighborAtypeLatency(
                db_path, neo4jPageCacheMemory, output_file);
        } else {
            throw new IllegalArgumentException("Unknown bench type: " + type);
        }
    }

    private static void benchNeighborAtypeLatency(
        String db_path, String neo4jPageCacheMem, String output_file) {

        List<Long> neighbors;

        System.out.println("Benchmarking getNeighborAtype queries");
        //System.out.println("Setting neo4j's dbms.pagecache.memory: " + neo4jPageCacheMem);
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(db_path)
            .setConfig(GraphDatabaseSettings.cache_type, "none")
            .setConfig(
                GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
            .newGraphDatabase();

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

            BenchUtils.fullWarmup(graphDb);
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
                    atypeMap[warmupAtypes.get(i % warmupAtypes.size())]);
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
                    atypeMap[queryAtypes.get(i % queryAtypes.size())]);

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

    public static List<Long> getNeighbors(
        GraphDatabaseService graphDb, long id, RelationshipType relType) {

        List<Long> neighbors = new ArrayList<>();
        Node n = graphDb.getNodeById(id);
        Iterable<Relationship> rels = n.getRelationships(
            relType, Direction.OUTGOING);
        for (Relationship r : rels) {
            neighbors.add(r.getOtherNode(n).getId());
        }
        return neighbors;
    }

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

}
