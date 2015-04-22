import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class BenchNode {
    private static final long WARMUP_TIME = (long) (60 * 1E9); // 60 seconds
    private static final long MEASURE_TIME = (long) (120 * 1E9); // 120 seconds
    private static final long COOLDOWN_TIME = (long) (30 * 1E9); // 30 seconds

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        int warmup_n = Integer.parseInt(args[5]);
        int measure_n = Integer.parseInt(args[6]);
        int cooldown_n = Integer.parseInt(args[7]);

        List<Integer> warmupAttributes = new ArrayList<Integer>();
        List<String> warmupQueries = new ArrayList<String>();
        List<Integer> attributes = new ArrayList<Integer>();
        List<String> queries = new ArrayList<String>();
        getQueries(warmup_query_path, warmupAttributes, warmupQueries);
        getQueries(query_path, attributes, queries);

        if (type.equals("node-throughput"))
            nodeThroughput(db_path, warmupAttributes, warmupQueries, attributes, queries, output_file);
        else if (type.equals("node-latency"))
            nodeLatency(db_path, warmup_n, measure_n, cooldown_n,
                warmupAttributes, warmupQueries, attributes, queries, output_file);
        else
            System.out.println("No type " + type + " is supported!");
    }

    private static void nodeLatency(String DB_PATH,
            int warmup_n, int measure_n, int cooldown_n,
            List<Integer> warmupAttributes, List<String> warmupQueries,
            List<Integer> attributes, List<String> queries, String output_file) {

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graphDb);
        IndexDefinition indexDefinition;
        Label label = DynamicLabel.label("Node");
        Transaction tx = graphDb.beginTx();
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));

            // warmup
            System.out.println("Warming up queries");
            int warmupSize = warmupAttributes.size();
            for (int i = 0; i < warmup_n; i++) {
                List<Long> nodes = getNodes(graphDb, label, warmupAttributes.get(i % warmupSize),
                        warmupQueries.get(i % warmupSize));
                if (nodes.size() == 0) {
                    System.out.println("Error: no results for attribute " + warmupAttributes.get(i)
                            + ", searching for " + warmupQueries.get(i));
                    System.exit(0);
                }
            }

            // measure
            System.out.println("Measure queries");
            int size = attributes.size();
            for (int i = 0; i < measure_n; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
                int attr = attributes.get(i % size);
                String query = queries.get(i % size);
                long queryStart = System.nanoTime();
                List<Long> nodes = getNodes(graphDb, label, attr, query);
                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(attr + "," + query + "," + nodes.size() + "," +  microsecs);
            }

            // cooldown
            for (int i = 0; i < cooldown_n; i++) {
                List<Long> nodes = getNodes(graphDb, label, warmupAttributes.get(i % warmupSize),
                        warmupQueries.get(i % warmupSize));
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

    private static void nodeThroughput(String DB_PATH,
            List<Integer> warmupAttributes, List<String> warmupQueries,
            List<Integer> attributes, List<String> queries, String output_file) {

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graphDb);
        IndexDefinition indexDefinition;
        Label label = DynamicLabel.label("Node");
        try (Transaction tx = graphDb.beginTx()) {
            // warmup
            int i = 0;
            System.out.println("Warming up queries");
            long warmupStart = System.nanoTime();
            int warmupSize = warmupAttributes.size();
            while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                List<Long> nodes = getNodes(graphDb, label, warmupAttributes.get(i % warmupSize),
                        warmupQueries.get(i % warmupSize));
                if (nodes.size() == 0) {
                    System.out.println("wtf " + warmupAttributes.get(i) + " " + warmupQueries.get(i));
                    System.exit(0);
                }
                i++;
            }

            // measure
            i = 0;
            System.out.println("Measure queries");
            long edges = 0;
            double totalSeconds = 0;
            long start = System.nanoTime();
            int size = attributes.size();
            while (System.nanoTime() - start < MEASURE_TIME) {
                long queryStart = System.nanoTime();
                List<Long> nodes = getNodes(graphDb, label, attributes.get(i % size),
                        queries.get(i % size));
                long queryEnd = System.nanoTime();
                totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
                i++;
            }
            double thput = ((double) i) / totalSeconds;

            System.out.println("Get Name throughput: " + thput);
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file, true)))) {
                out.println(DB_PATH);
                out.println(thput + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }

            // cooldown
            i = 0;
            long cooldownStart = System.nanoTime();
            while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                List<Long> nodes = getNodes(graphDb, label, warmupAttributes.get(i % warmupSize),
                        warmupQueries.get(i % warmupSize));
                i++;
            }
            tx.success();
        }
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    private static List<Long> getNodes(GraphDatabaseService graphDb,
            Label label, int attr, String search) {
	ResourceIterator<Node> nodes = graphDb.findNodes(label, "name" + attr,search);
        try {
            ArrayList<Long> userIds = new ArrayList<>();
            while (nodes.hasNext()) {
                userIds.add(nodes.next().getId());
            }
            return userIds;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void getQueries(String file, List<Integer> indices, List<String> queries) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] tokens = line.split(",");
                indices.add(Integer.parseInt(tokens[0]));
                queries.add(tokens[1]);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
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
