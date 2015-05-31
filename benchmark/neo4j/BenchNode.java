import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

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

    private static int WARMUP_N = 100000;
    private static int MEASURE_N = 100000;
    private static int COOLDOWN_N = 500;

    public static void main(String[] args) {
        String type = args[0];
        String db_path = args[1];
        String warmup_query_path = args[2];
        String query_path = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);

        List<Integer> warmupAttributes1 = new ArrayList<Integer>();
        List<Integer> warmupAttributes2 = new ArrayList<Integer>();
        List<String> warmupQueries1 = new ArrayList<String>();
        List<String> warmupQueries2 = new ArrayList<String>();
        List<Integer> attributes1 = new ArrayList<Integer>();
        List<Integer> attributes2 = new ArrayList<Integer>();
        List<String> queries1 = new ArrayList<String>();
        List<String> queries2 = new ArrayList<String>();
        getQueries(warmup_query_path, warmupAttributes1, warmupAttributes2, warmupQueries1, warmupQueries2);
        getQueries(query_path, attributes1, attributes2, queries1, queries2);

        if (type.equals("node-throughput"))
            nodeThroughput(db_path, warmupAttributes1, warmupQueries1, attributes1, queries1, output_file);
        else if (type.equals("node-latency"))
            nodeLatency(db_path, warmupAttributes1, warmupQueries1, attributes1, queries1, output_file);
        else if (type.equals("node-node-latency"))
            nodeNodeLatency(db_path, warmupAttributes1, warmupAttributes2, warmupQueries1, warmupQueries2,
                    attributes1, attributes2, queries1, queries2, output_file);
        else
            System.out.println("No type " + type + " is supported!");
    }

    private static void nodeLatency(String DB_PATH,
            List<Integer> warmupAttributes, List<String> warmupQueries,
            List<Integer> attributes, List<String> queries, String output_file) {

        System.out.println("Benchmarking getNode queries");
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
            System.out.println("Warming up for " + WARMUP_N + " queries");
            int warmupSize = warmupAttributes.size();
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
                List<Long> nodes = getNodes(graphDb, label, warmupAttributes.get(i % warmupSize),
                        warmupQueries.get(i % warmupSize));
                if (nodes.size() == 0) {
                    System.out.println("Error: no results for attribute " + warmupAttributes.get(i)
                            + ", searching for " + warmupQueries.get(i));
                    System.exit(0);
                }
            }

            // measure
            System.out.println("Measuring for " + MEASURE_N + " queries");
            int size = attributes.size();
            for (int i = 0; i < MEASURE_N; i++) {
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
                out.println(nodes.size() + "," +  microsecs);
            }

            // cooldown
            System.out.println("Cooldown for " + COOLDOWN_N + " queries");
            for (int i = 0; i < COOLDOWN_N; i++) {
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

    private static void nodeNodeLatency(String DB_PATH,
            List<Integer> warmupAttributes1, List<Integer> warmupAttributes2,
            List<String> warmupQueries1, List<String> warmupQueries2,
            List<Integer> attributes1, List<Integer> attributes2,
            List<String> queries1, List<String> queries2, String output_file) {

        System.out.println("Benchmarking getNodeNode queries");
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
            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
                List<Long> nodes = getNodes(graphDb, label,
                        warmupAttributes1.get(i), warmupQueries1.get(i),
                        warmupAttributes2.get(i), warmupQueries2.get(i));
                if (nodes.size() == 0) {
                    System.out.printf("Error: no results for attr1 %d search %s, attr2 %d, search %s\n",
                            warmupAttributes1.get(i), warmupQueries1.get(i), warmupAttributes2.get(i), warmupQueries2.get(i));
                    System.exit(0);
                }
            }

            // measure
            System.out.println("Measuring for " + MEASURE_N + " queries");
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
                long queryStart = System.nanoTime();
                List<Long> nodes = getNodes(graphDb, label,
                        attributes1.get(i), queries1.get(i),
                        attributes2.get(i), queries2.get(i));
                long queryEnd = System.nanoTime();
                double microsecs = (queryEnd - queryStart) / ((double) 1000);
                out.println(nodes.size() + "," +  microsecs);
            }

            // cooldown
            System.out.println("Cooldown for " + COOLDOWN_N + " queries");
            for (int i = 0; i < COOLDOWN_N; i++) {
                List<Long> nodes = getNodes(graphDb, label,
                        warmupAttributes1.get(i), warmupQueries1.get(i),
                        warmupAttributes2.get(i), warmupQueries2.get(i));
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

    private static List<Long> getNodes(GraphDatabaseService graphDb,
            Label label, int attr1, String search1, int attr2, String search2) {
        ResourceIterator<Node> nodes = graphDb.findNodes(label, "name" + attr1, search1);
        ResourceIterator<Node> nodes2 = graphDb.findNodes(label, "name" + attr2, search2);
        try {
            Set<Long> s1 = new HashSet<Long>();
            while (nodes.hasNext()) {
                s1.add(nodes.next().getId());
            }
            Set<Long> s2 = new HashSet<Long>();
            while (nodes2.hasNext()) {
                s2.add(nodes2.next().getId());
            }
            s1.retainAll(s2);
            List<Long> ans = new ArrayList<>(s1);
            return ans;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void getQueries(String file, List<Integer> indices1, List<Integer> indices2,
            List<String> queries1, List<String> queries2) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] tokens = line.split(",");
                indices1.add(Integer.parseInt(tokens[0]));
                queries1.add(tokens[1]);
                indices2.add(Integer.parseInt(tokens[2]));
                queries2.add(tokens[3]);
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
