package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.*;
import java.util.*;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.modGet;

public class NeighborNodeBench {
    private static int WARMUP_N = 20000;
    private static int MEASURE_N = 100000;

    private static Label NODE_LABEL = DynamicLabel.label("Node");

    public static void main(String[] args) throws Exception {
        String type = args[0];
        String db_path = args[1];
        String warmup_file = args[2];
        String query_file = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        String neo4jPageCacheMemory;
        if (args.length >= 8) neo4jPageCacheMemory = args[7];
        else neo4jPageCacheMemory = GraphDatabaseSettings.pagecache_memory.getDefaultValue();

        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output_file)));
        PrintWriter resOut = null;
        if (System.getenv("BENCH_PRINT_RESULTS") != null) {
            resOut = new PrintWriter(new BufferedWriter(
                new FileWriter(output_file + ".neo4j_result")));
        }

        List<Integer> warmup_neighbor_indices = new ArrayList<Integer>();
        List<Integer> warmup_node_attributes = new ArrayList<Integer>();
        List<String> warmup_node_queries = new ArrayList<String>();
        getNeighborNodeQueries(
            warmup_file, warmup_neighbor_indices, warmup_node_attributes, warmup_node_queries);

        List<Integer> neighbor_indices = new ArrayList<Integer>();
        List<Integer> node_attributes = new ArrayList<Integer>();
        List<String> node_queries = new ArrayList<String>();
        getNeighborNodeQueries(query_file, neighbor_indices, node_attributes, node_queries);

        if (type.equals("latency")) {
            neighborNodeLatency(db_path, neo4jPageCacheMemory, out, resOut, warmup_neighbor_indices, neighbor_indices,
                warmup_node_attributes, warmup_node_queries, node_attributes, node_queries, false);
        } else if (type.equals("latency-index")) {
            neighborNodeLatency(db_path, neo4jPageCacheMemory, out, resOut, warmup_neighbor_indices, neighbor_indices,
                warmup_node_attributes, warmup_node_queries, node_attributes, node_queries, true);
        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    private static void neighborNodeLatency(
        String DB_PATH, String neo4jPageCacheMemory, PrintWriter out, PrintWriter resOut,
        List<Integer> warmup_neighbor_indices, List<Integer> neighbor_indices,
        List<Integer> warmup_node_attributes, List<String> warmup_node_queries,
        List<Integer> node_attributes, List<String> node_queries,
        boolean useIndex) {

        System.out.println("Benchmarking getNeighborNode queries");
        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new GraphDatabaseFactory()
            .newEmbeddedDatabaseBuilder(DB_PATH)
            .setConfig(GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMemory)
            .newGraphDatabase();

        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = graphDb.beginTx();
        try {
            BenchUtils.awaitIndexes(graphDb);

            // warmup
            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                List<Long> result;
                if (!useIndex) {
                    result = getNeighborNode(graphDb, modGet(warmup_neighbor_indices, i),
                        modGet(warmup_node_attributes, i), modGet(warmup_node_queries, i));
                } else {
                    result = getNeighborNodeUsingIndex(graphDb, modGet(warmup_neighbor_indices, i),
                        modGet(warmup_node_attributes, i), modGet(warmup_node_queries, i));
                }
                if (result.size() == 0) {
//                    System.out.printf(
//                        "Error: no neighbor nodes for node id: %d, attr %d, search %s\n",
//                        modGet(warmup_neighbor_indices, i),
//                        modGet(warmup_node_attributes, i),
//                        modGet(warmup_node_queries, i));
                    // For now, just ignore...
                    // System.exit(0);
                }
            }

            // measure
            System.out.println("Measuring for " + MEASURE_N + " queries");
            for (int i = 0; i < MEASURE_N; i++) {
                if (i % 10000 == 0) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                }
                long queryStart, queryEnd;
                List<Long> result;
                if (!useIndex) {
                    queryStart = System.nanoTime();
                    result = getNeighborNode(graphDb, modGet(neighbor_indices, i),
                        modGet(node_attributes, i), modGet(node_queries, i));
                    queryEnd = System.nanoTime();
                } else {
                    queryStart = System.nanoTime();
                    result = getNeighborNodeUsingIndex(graphDb, modGet(neighbor_indices, i),
                        modGet(node_attributes, i), modGet(node_queries, i));
                    queryEnd = System.nanoTime();
                }
                if (result.size() == 0) {
//                    System.out.printf(
//                        "Error: no neighbor nodes for node id: %d, attr %d, search %s\n",
//                        modGet(neighbor_indices, i),
//                        modGet(node_attributes, i),
//                        modGet(node_queries, i));
                } else {
                    out.println(result.size() + "," + (queryEnd - queryStart) / 1000);
                }

                // correctness check
                if (resOut != null) {
                    String header = String.format("id %d attr %d query %s",
                        modGet(neighbor_indices, i),
                        modGet(node_attributes, i),
                        modGet(node_queries, i));
                    Collections.sort(result);
                    BenchUtils.print(header, result, resOut);
                }
            }
            tx.success();
        } finally {
            out.close();
            if (resOut != null) resOut.close();
            tx.close();
        }
        System.out.println("Shutting down database ...");
        graphDb.shutdown();
    }

    private static List<Long> getNeighborNode(GraphDatabaseService graphDb,
                                              long node_id, int attr, String search) {
        Node n = graphDb.getNodeById(node_id);
        List<Long> result = new LinkedList<>();
        for (Relationship r : n.getRelationships(Direction.OUTGOING)) {
            Node neighbor = r.getOtherNode(n);
            if (search.equals(neighbor.getProperty("name" + attr))) {
                result.add(neighbor.getId());
            }
        }
        return result;
    }

    // slow
    private static List<Long> getNeighborNodeUsingIndex(
        GraphDatabaseService graphDb, long node_id, int attr, String search) {

        Set<Long> neighbors = new HashSet<Long>();
        Node n = graphDb.getNodeById(node_id);
        for (Relationship rel : n.getRelationships(Direction.OUTGOING)) {
            neighbors.add(rel.getOtherNode(n).getId());
        }
//        System.out.println("getting nbhr done");
        List<Long> result = new LinkedList<Long>();

        // .findNodes() *should* be able to use index
        try (ResourceIterator<Node> nodes = graphDb.findNodes(
            NODE_LABEL, "name" + attr, search)) {
            while (nodes.hasNext()) {
                long validNode = nodes.next().getId();
                if (neighbors.contains(validNode))
                    result.add(validNode);
            }
        }
        return result;
    }

    // cypher version: slow
    private static List<Long> getNeighborNodeUsingIndex0(
        GraphDatabaseService graphDb, long node_id, int attr, String search) {
        List<Long> result = new LinkedList<Long>();

        String queryString = String.format("START n=node(%d)\n" +
            "MATCH (m:Node)\n" +
            "WHERE m.name%d = '%s' AND n-->m\n" +
            "RETURN m;\n" +
            "\n", node_id, attr, search);

        try ( Result queryResult = graphDb.execute(queryString) ) {
            while (queryResult.hasNext()) {
                Map<String,Object> row = queryResult.next();
                for (Map.Entry<String, Object> column : row.entrySet()) {
                    result.add(((Node) column.getValue()).getId());
                }
            }
        }
//        System.out.println("Query: " + queryString);
//        System.out.println("Result:\n" + queryResult.resultAsString());
//        System.out.println("Plan:\n" + queryResult.getExecutionPlanDescription());
        return result;
    }

    private static void getNeighborNodeQueries(
        String file, List<Integer> indices, List<Integer> attributes, List<String> queries) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] tokens = line.split(",");
                indices.add(Integer.parseInt(tokens[0]));
                attributes.add(Integer.parseInt(tokens[1]));
                queries.add(tokens[2]);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
