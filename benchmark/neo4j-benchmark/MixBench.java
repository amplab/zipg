import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class MixBench {
    private static int WARMUP_N = 20000;
    private static int MEASURE_N = 100000;
    private static int COOLDOWN_N = 1000;

    public static void main(String[] args) {
        String db_path = args[0];
        String warmup_node_file = args[1];
        String query_node_file = args[2];
        String warmup_neighbor_file = args[3];
        String query_neighbor_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        COOLDOWN_N = Integer.parseInt(args[7]);
        String output_file = args[8];
        nameThroughput(db_path, warmup_node_file, query_node_file,
            warmup_neighbor_file, query_neighbor_file, output_file);
    }

    private static void nameThroughput(String DB_PATH,
            String warmup_node_file, String query_node_file,
            String warmup_neighbor_file, String query_neighbor_file, String output_file) {

        List<Integer> warmup_neighbor_indices = new ArrayList<Integer>();
        List<Integer> neighbor_indices = new ArrayList<Integer>();
        getNeighborQueries(warmup_neighbor_file, warmup_neighbor_indices);
        getNeighborQueries(query_neighbor_file, neighbor_indices);

        List<Integer> warmup_node_attributes = new ArrayList<Integer>();
        List<String> warmup_node_queries = new ArrayList<String>();
        List<Integer> node_attributes = new ArrayList<Integer>();
        List<String> node_queries = new ArrayList<String>();
        getNodeQueries(warmup_node_file, warmup_node_attributes, warmup_node_queries);
        getNodeQueries(query_node_file, node_attributes, node_queries);

        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(graphDb);
        IndexDefinition indexDefinition;
        Label label = DynamicLabel.label("Node");
        Transaction tx = graphDb.beginTx();
        try {
            // warmup
            System.out.println("Warming up queries");
            int warmup_size = warmup_neighbor_indices.size();
            int size = neighbor_indices.size();
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    List<Node> neighbors = getNeighbors(graphDb, warmup_neighbor_indices.get(i/2 % warmup_size));
                    if (neighbors.size() == 0) {
                        System.out.println("error: no neighbors for node " + warmup_neighbor_indices.get(i/2 % warmup_size));
                        System.exit(0);
                    }
                } else {
                    List<Long> result = getNodes(graphDb, label, warmup_node_attributes.get(i/2 % warmup_size),
                            warmup_node_queries.get(i/2 % warmup_size));
                    if (result.size() == 0) {
                        System.out.println("error: no nodes for query " + warmup_node_attributes.get(i/2 % warmup_size) + " " + warmup_node_queries.get(i/2 % warmup_size));
                        System.exit(0);
                    }
                }
                if (i > 0 && i % 1000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
            }

            // measure
            System.out.println("Measure queries");
            double totalSeconds = 0;
            for (int i = 0; i < MEASURE_N; i++) {
                long queryStart; long queryEnd;
                if (i % 2 == 0) {
                    int idx = neighbor_indices.get(i/2 % size);
                    queryStart = System.nanoTime();
                    List<Node> neighbors = getNeighbors(graphDb, idx);
                    queryEnd = System.nanoTime();
                } else {
                    int attr = node_attributes.get(i/2 % size);
                    String query = node_queries.get(i/2 % size);
                    queryStart = System.nanoTime();
                    List<Long> results = getNodes(graphDb, label, attr, query);
                    queryEnd = System.nanoTime();
                }
	        totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
                if (i > 0 && i % 1000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
            }
            double thput = ((double) MEASURE_N) / totalSeconds;

            System.out.println("Mixed throughput: " + thput);
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file, true)))) {
                out.println(DB_PATH);
                out.println("throughput: " + thput);
                out.println("queries: " + MEASURE_N + ", total time: " + totalSeconds + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }

            // cooldown
            for (int i = 0; i < COOLDOWN_N; i++) {
                if (i % 2 == 0) {
                    List<Node> neighbors = getNeighbors(graphDb, warmup_neighbor_indices.get(i/2 % warmup_size));
                } else {
                    List<Long> results = getNodes(graphDb, label, warmup_node_attributes.get(i/2 % warmup_size),
                            warmup_node_queries.get(i/2 % warmup_size));
                }
                if (i > 0 && i % 1000 == 0) {
                    tx.success();
                    tx.finish();
                    tx = graphDb.beginTx();
                }
            }
            tx.success();
        } finally {
            tx.finish();
        }
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    private static List<Node> getNeighbors(GraphDatabaseService graphDb, long id) {
        Node n = graphDb.getNodeById(id);
        List<Node> neighbors = new LinkedList<>();
        for (Relationship r : n.getRelationships(Direction.OUTGOING)) {
            neighbors.add(r.getOtherNode(n));
        }
        return neighbors;
    }

    private static List<Long> getNodes(GraphDatabaseService graphDb,
            Label label, int attr, String search) {
        try (ResourceIterator<Node> nodes = graphDb.findNodes(label, "name" + attr,
                search)) {
            ArrayList<Long> userIds = new ArrayList<>();
            while (nodes.hasNext()) {
                userIds.add(nodes.next().getId());
            }
            return userIds;
        }
    }

    private static void getNeighborQueries(String file, List<Integer> indices) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                indices.add(Integer.parseInt(line));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getNodeQueries(String file, List<Integer> indices, List<String> queries) {
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
