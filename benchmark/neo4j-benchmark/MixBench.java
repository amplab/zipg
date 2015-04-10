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

public class MixBench {
    private static final int WARMUP_N = 500000;
    private static final int MEASURE_N = 2000000;
    private static final int COOLDOWN_N = 100000;

    public static void main(String[] args) {
        String db_path = args[0];
        String warmup_node_file = args[1];
        String query_node_file = args[2];
        String warmup_neighbor_file = args[3];
        String query_neighbor_file = args[4];
        String output_file = args[5];
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
        try (Transaction tx = graphDb.beginTx()) {
            List<Node> tempNodes = new LinkedList<Node>();
            for (Node n : graphDb.getAllNodes()) {
                tempNodes.add(n);
            }
            int N = tempNodes.size();

            Node[] nodes = new Node[N];
            for (int i = 0; i < N; i++) {
                nodes[i] = tempNodes.remove(0);
            }

            // warmup
            System.out.println("Warming up queries");
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    List<Node> neighbors = getNeighbors(node[warmup_neighbor_indices.get(i/2)]);
                    if (neighbors.size() == 0) {
                        System.out.println("error: no neighbors for node " + warmup_neighbor_indices.get(i));
                        System.exit(0);
                    }
                } else {
                    List<Long> nodes = getNodes(graphDb, label, warmup_node_attributes.get(i/2),
                            warmup_node_queries.get(i/2));
                    if (nodes.size() == 0) {
                        System.out.println("error: no nodes for query " + warmup_node_attributes.get(i) + " " + warmup_node_queries.get(i));
                        System.exit(0);
                    }
                }
            }

            // measure
            System.out.println("Measure queries");
            double totalSeconds = 0;
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 2 == 0) {
                    Node n = neighbor_indices.get(i/2);
                    long queryStart = System.nanoTime();
                    List<Node> neighbors = getNeighbors(n);
                    long queryEnd = System.nanoTime();
                    totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
                } else {
                    int attr = node_attributes.get(i/2);
                    String query = node_queries.get(i/2);
                    long queryStart = System.nanoTime();
                    List<Long> nodes = getNodes(graphDb, label, attr, query);
                    long queryEnd = System.nanoTime();
                    totalSeconds += (queryEnd - queryStart) / ((double) 1E9);
                }
            }
            double thput = ((double) MEASURE_N) / totalSeconds;

            System.out.println("Mixed throughput: " + thput);
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file, true)))) {
                out.println(DB_PATH);
                out.println(thput + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }

            // cooldown
            for (int i = 0; i < WARMUP_N; i++) {
                if (i % 2 == 0) {
                    List<Node> neighbors = getNeighbors(node[warmup_neighbor_indices.get(i/2)]);
                } else {
                    List<Long> nodes = getNodes(graphDb, label, warmup_node_attributes.get(i/2),
                            warmup_node_queries.get(i/2));
                }
            }
            tx.success();
        }
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    private static List<Node> getNeighbors(Node n) {
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
