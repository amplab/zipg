import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class BenchNeighbor {
    private static final long WARMUP_TIME = (long) (60 * 1E9); // 60 seconds
    private static final long MEASURE_TIME = (long) (120 * 1E9); // 120 seconds
    private static final long COOLDOWN_TIME = (long) (10 * 1E9); // 10 seconds

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        int warmup_n = Integer.parseInt(args[5]);
        int measure_n = Integer.parseInt(args[6]);
        int cooldown_n = Integer.parseInt(args[7]);
        int[] warmupQueries = getQueries(warmup_query_path);
        int[] queries = getQueries(query_path);
        if (type.equals("neighbor-latency"))
            benchNeighborLatency(db_path, warmup_n, measure_n, cooldown_n, warmupQueries, queries, output_file);
        else if (type.equals("neighbor-throughput"))
            benchNeighborThroughput(db_path, warmupQueries, queries, output_file);
    }

    private static void benchNeighborLatency(String db_path,
            int warmup_n, int measure_n, int cooldown_n,
            int[] warmupQueries, int[] queries, String output_file) {

        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(db_path);
        registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
            // warmup
            for (int i = 0; i < warmup_n; i++) {
                List<Node> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
                if (neighbors.size() == 0) {
                    System.err.println("Error: no results for neighbor of " + warmupQueries[i % warmupQueries.length]);
                }
            }

            // measure
            for (int i = 0; i < measure_n; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
                long queryStart = System.nanoTime();
                List<Node> neighbors = getNeighbors(graphDb, queries[i % queries.length]);
                long queryEnd = System.nanoTime();
                double millisecs = (queryEnd - queryStart) / ((double) 1000 * 1000);
                out.println(queries[i % queries.length] + "," + neighbors.size() + "," + millisecs);
            }

            // cooldown
            for (int i = 0; i < cooldown_n; i++) {
                List<Node> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
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
        registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file, true)));
            // warmup
            System.out.println("Warming up neo4j neighbor throughput");
            int i = 0;
            long warmupStart = System.nanoTime();
            while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                List<Node> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
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
                    tx.finish();
                    tx = graphDb.beginTx();
                }
                long queryStart = System.nanoTime();
                List<Node> neighbors = getNeighbors(graphDb, queries[i % queries.length]);
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
                List<Node> neighbors = getNeighbors(graphDb, warmupQueries[i % warmupQueries.length]);
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

    private static List<Node> getNeighbors(GraphDatabaseService graphDb, long id) {
        Node n = graphDb.getNodeById(id);
        List<Node> neighbors = new LinkedList<>();
        for (Relationship r : n.getRelationships(Direction.OUTGOING)) {
            neighbors.add(r.getOtherNode(n));
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

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
