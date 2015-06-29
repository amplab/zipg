package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BenchNeighborAtype {

    static class AType implements RelationshipType {
        String name_;

        public AType(String name) {
            this.name_ = name;
        }

        public String name() {
            return this.name_;
        }
    }

    private static int WARMUP_N;
    private static int MEASURE_N;

    private static List<Long> warmupIds, queryIds;
    private static List<Integer> warmupAtypes, queryAtypes;

    // assume 5 atypes
    private static RelationshipType[] atypeMap = new RelationshipType[5];

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        String neo4jPageCacheMemory;
        if (args.length >= 8) neo4jPageCacheMemory = args[7];
        else neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory.getDefaultValue();

        warmupIds = new ArrayList<>();
        queryIds = new ArrayList<>();
        warmupAtypes = new ArrayList<>();
        queryAtypes = new ArrayList<>();

        // assume 5 atypes
        for (int i = 0; i < 5; ++i) {
            atypeMap[i] = new AType(String.valueOf(i));
        }

        getQueries(warmup_query_path, warmupIds, warmupAtypes);
        getQueries(query_path, queryIds, queryAtypes);

        if (type.equals("latency")) {
            benchNeighborAtypeLatency(
                db_path, neo4jPageCacheMemory, output_file);
        } else {
            throw new IllegalArgumentException("Unknown bench type: " + type);
        }
    }

    private static void benchNeighborAtypeLatency(
        String db_path,
        String neo4jPageCacheMem,
        String output_file) {

        List<Long> neighbors;

        System.out.println("Benchmarking getNeighbor queries");
        //System.out.println("Setting neo4j's dbms.pagecache.memory: " + neo4jPageCacheMem);
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(db_path)
                //.setConfig(GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem)
            .newGraphDatabase();

        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
            PrintWriter resOut = null;
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = new PrintWriter(new BufferedWriter(
                    new FileWriter(output_file + ".neo4j_result")));
            }

            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                getNeighbors(
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

                neighbors = getNeighbors(
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
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    private static List<Long> getNeighbors(
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

    private static void getQueries(
        String file, List<Long> nodeIds, List<Integer> atypes) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] toks = line.split(",");
                nodeIds.add(Long.valueOf(toks[0]));
                atypes.add(Integer.valueOf(toks[1]));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
