package benchmark.neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static benchmark.neo4j.BenchUtils.*;

public class BenchNeighbor {
    private static final long WARMUP_TIME = (long) (60 * 1E9); // 60 seconds
    private static final long MEASURE_TIME = (long) (120 * 1E9); // 120 seconds
    private static final long COOLDOWN_TIME = (long) (10 * 1E9); // 10 seconds

    private static int WARMUP_N = 500000;
    private static int MEASURE_N = 500000;
    private static int COOLDOWN_N = 500;

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);

        int[] warmupQueries = getQueries(warmup_query_path);
        int[] queries = getQueries(query_path);
        if (type.equals("neighbor-latency"))
            benchNeighborLatency(db_path, warmupQueries, queries, output_file);
        else if (type.equals("neighbor-throughput"))
            benchNeighborThroughput(db_path, warmupQueries, queries, output_file);
    }

    private static void benchNeighborLatency(String db_path,
            int[] warmupQueries, int[] queries, String output_file) {

        System.out.println("Benchmarking getNeighbor queries");
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(db_path);
        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));

            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                List<Long> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
                if (neighbors.size() == 0) {
                    System.err.println("Error: no results for neighbor of " + warmupQueries[i % warmupQueries.length]);
                }
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
                List<Long> neighbors = getNeighbors(graphDb, queries[i % queries.length]);
                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(neighbors.size() + "," + microsecs);
            }

            // cooldown
            System.out.println("Cooldown for " + COOLDOWN_N + " queries");
            for (int i = 0; i < COOLDOWN_N; i++) {
                List<Long> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
            }

            tx.success();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    private static void benchNeighborThroughput(String db_path,
            int[] warmupQueries, int[] queries, String output_file) {

        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(db_path);
        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file, true)));
            // warmup
            System.out.println("Warming up neo4j neighbor throughput");
            int i = 0;
            long warmupStart = System.nanoTime();
            while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                List<Long> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
                if (neighbors.size() == 0)
                    System.err.println("Error: no results found in neo4j neighbor throughput benchmarking");
                i++;
            }

            // measure
            System.out.println("Measuring neo4j neighbor throughput");
            i = 0;
            long edges = 0;
            double totalSeconds = 0;
            long start = System.nanoTime();
            while (System.nanoTime() - start < MEASURE_TIME) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                long queryStart = System.nanoTime();
                List<Long> neighbors = getNeighbors(graphDb, queries[i % queries.length]);
                long queryEnd = System.nanoTime();
                if (neighbors.size() == 0)
                    System.err.println("Error: no results found in neo4j neighbor throughput benchmarking");

                totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
                edges += neighbors.size();
                i++;
            }
            double getNeighborThput = ((double) i) / totalSeconds;
            double edgesThput = ((double) edges) / totalSeconds;

            // cooldown
            System.out.println("cooling down neo4j neighbor throughput");
            i = 0;
            long cooldownStart = System.nanoTime();
            while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                List<Long> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
                i++;
            }

            System.out.println("neighbor throughput: " + getNeighborThput);
            System.out.println("edge throughput: " + edgesThput);

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    private static List<Long> getNeighbors(GraphDatabaseService graphDb, long id) {
        List<Long> neighbors = new LinkedList<>();
        Node n = graphDb.getNodeById(id);
        Iterable<Relationship> rels = n.getRelationships(Direction.OUTGOING);
        for (Relationship r : rels) {
            neighbors.add(r.getOtherNode(n).getId());
        }
        return neighbors;
    }


    private static int[] getQueries(String file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            List<String> lines = new ArrayList<String>();
            String line = br.readLine();
            while (line != null) {
                lines.add(line);
                line = br.readLine();
            }
            int[] queries = new int[lines.size()];
            for (int i = 0; i < lines.size(); i++) {
                queries[i] = Integer.parseInt(lines.get(i));
            }
            return queries;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
