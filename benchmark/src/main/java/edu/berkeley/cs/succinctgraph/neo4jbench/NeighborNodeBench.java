package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.*;
import java.util.*;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.fullWarmup;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.modGet;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.printMemoryFootprint;

public class NeighborNodeBench {

    private static int WARMUP_N = 20000;
    private static int MEASURE_N = 100000;

    private static List<Long> warmupNeighborIndices = new ArrayList<Long>();
    private static List<Integer> warmupNodeAttributes = new ArrayList<Integer>();
    private static List<String> warmupNodeQueries = new ArrayList<String>();

    private static List<Long> neighborIndices = new ArrayList<Long>();
    private static List<Integer> nodeAttributes = new ArrayList<Integer>();
    private static List<String> nodeQueries = new ArrayList<String>();

    private static Label NODE_LABEL = DynamicLabel.label("Node");

    public static void main(String[] args) throws Exception {
        String type = args[0];
        String db_path = args[1];
        String warmup_file = args[2];
        String query_file = args[3];
        String output_file = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        int numClients = Integer.parseInt(args[7]);
        boolean tuned = Boolean.valueOf(args[8]);
        String neo4jPageCacheMem = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length >= 10) {
            neo4jPageCacheMem = args[9];
        }

        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
            output_file)));
        PrintWriter resOut = null;
        if (System.getenv("BENCH_PRINT_RESULTS") != null) {
            resOut = new PrintWriter(new BufferedWriter(
                new FileWriter(output_file + ".neo4j_result")));
        }

        BenchUtils.getNeighborNodeQueries(
            warmup_file, warmupNeighborIndices, warmupNodeAttributes,
            warmupNodeQueries);
        BenchUtils.getNeighborNodeQueries(query_file, neighborIndices,
            nodeAttributes, nodeQueries);

        if (type.equals("latency")) {

            neighborNodeLatency(
                tuned, db_path, neo4jPageCacheMem, out, resOut, false);

        } else if (type.equals("latency-index")) {

            neighborNodeLatency(
                tuned, db_path, neo4jPageCacheMem, out, resOut, true);

        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    private static void neighborNodeLatency(boolean tuned,
        String dbPath, String neo4jPageCacheMem,
        PrintWriter out, PrintWriter resOut, boolean useIndex) {

        System.out.println("Benchmarking getNeighborNode queries");
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
            BenchUtils.awaitIndexes(graphDb);

            // warmup
            if (tuned) {
                fullWarmup(graphDb);
            }
            System.out.println("Warming up for " + WARMUP_N + " queries");
            for (int i = 0; i < WARMUP_N; i++) {
                List<Long> result;
                if (!useIndex) {
                    result = getNeighborNode(graphDb,
                        modGet(warmupNeighborIndices, i),
                        modGet(warmupNodeAttributes, i),
                        modGet(warmupNodeQueries, i));
                } else {
                    result = getNeighborNodeUsingIndex(graphDb,
                        modGet(warmupNeighborIndices, i),
                        modGet(warmupNodeAttributes, i),
                        modGet(warmupNodeQueries, i));
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
                    result = getNeighborNode(graphDb,
                        modGet(neighborIndices, i),
                        modGet(nodeAttributes, i), modGet(nodeQueries, i));
                    queryEnd = System.nanoTime();
                } else {
                    queryStart = System.nanoTime();
                    result = getNeighborNodeUsingIndex(graphDb,
                        modGet(neighborIndices, i),
                        modGet(nodeAttributes, i), modGet(nodeQueries, i));
                    queryEnd = System.nanoTime();
                }
//              if (result.size() == 0) {
//                    System.out.printf(
//                        "Error: no neighbor nodes for node id: %d, attr %d, search %s\n",
//                        modGet(neighbor_indices, i),
//                        modGet(node_attributes, i),
//                        modGet(node_queries, i));
                out.println(result.size() + "," +
                    (queryEnd - queryStart) / 1000);

                // correctness check
                if (resOut != null) {
                    String header = String.format("id %d attr %d query %s",
                        modGet(neighborIndices, i),
                        modGet(nodeAttributes, i),
                        modGet(nodeQueries, i));
                    Collections.sort(result);
                    BenchUtils.print(header, result, resOut);
                }
            }
            tx.success();
        } finally {
            out.close();
            if (resOut != null) resOut.close();
            tx.close();
            printMemoryFootprint();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    public static List<Long> getNeighborNode(
        GraphDatabaseService graphDb, long node_id, int attr, String search) {

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
    public static List<Long> getNeighborNodeUsingIndex(
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
    public static List<Long> getNeighborNodeUsingIndex0(
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
}
