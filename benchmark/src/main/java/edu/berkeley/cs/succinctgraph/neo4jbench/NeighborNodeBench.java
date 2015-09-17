package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchConstants.*;
import static edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils.*;

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
        String dbPath = args[1];
        String warmup_file = args[2];
        String query_file = args[3];
        String outputFile = args[4];
        WARMUP_N = Integer.parseInt(args[5]);
        MEASURE_N = Integer.parseInt(args[6]);
        int numClients = Integer.parseInt(args[7]);
        boolean tuned = Boolean.valueOf(args[8]);
        String neo4jPageCacheMem = GraphDatabaseSettings.pagecache_memory
            .getDefaultValue();
        if (args.length >= 10) {
            neo4jPageCacheMem = args[9];
        }

        BenchUtils.getNeighborNodeQueries(
            warmup_file, warmupNeighborIndices, warmupNodeAttributes,
            warmupNodeQueries);
        BenchUtils.getNeighborNodeQueries(query_file, neighborIndices,
            nodeAttributes, nodeQueries);

        if (type.equals("latency")) {

            neighborNodeLatency(
                tuned, dbPath, neo4jPageCacheMem, outputFile, false);

        } else if (type.equals("latency-index")) {

            neighborNodeLatency(
                tuned, dbPath, neo4jPageCacheMem, outputFile, true);

        } else if (type.equals("throughput")) {

            neighborNodeThroughput(
                tuned, dbPath, neo4jPageCacheMem, numClients);

        } else {
            System.out.println("No type " + type + " is supported!");
        }
    }

    static class RunNeighborNodeThroughput implements Runnable {
        private int clientId;
        private GraphDatabaseService graphDb;

        public RunNeighborNodeThroughput(
            int clientId, GraphDatabaseService graphDb) {

            this.clientId = clientId;
            this.graphDb = graphDb;
        }

        public void run() {
            Transaction tx = graphDb.beginTx();
            PrintWriter out = null;
            Random rand = new Random(1618 + clientId);
            try {
                // true for append
                out = new PrintWriter(new BufferedWriter(new FileWriter(
                    "neo4j_throughput_get_nhbrs_attr.txt", true)));

                // warmup
                int i = 0, queryIdx, warmupSize = warmupNeighborIndices.size();
                long warmupStart = System.nanoTime();
                while (System.nanoTime() - warmupStart < WARMUP_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(warmupSize);
                    getNeighborNode(graphDb,
                        modGet(warmupNeighborIndices, queryIdx),
                        modGet(warmupNodeAttributes, queryIdx),
                        modGet(warmupNodeQueries, queryIdx));
                    ++i;
                }

                // measure
                i = 0;
                long edges = 0;
                int querySize = neighborIndices.size();
                List<Long> neighbors;
                long start = System.nanoTime();
                while (System.nanoTime() - start < MEASURE_TIME) {
                    if (i % 10000 == 0) {
                        tx.success();
                        tx.close();
                        tx = graphDb.beginTx();
                    }
                    queryIdx = rand.nextInt(querySize);
                    neighbors =
                        getNeighborNode(graphDb,
                            modGet(neighborIndices, queryIdx),
                            modGet(nodeAttributes, queryIdx),
                            modGet(nodeQueries, queryIdx));
                    edges += neighbors.size();
                    ++i;
                }
                long end = System.nanoTime();
                double totalSeconds = (end - start) * 1. / 1e9;
                double queryThput = ((double) i) / totalSeconds;
                double edgesThput = ((double) edges) / totalSeconds;

                // cooldown
                long cooldownStart = System.nanoTime();
                while (System.nanoTime() - cooldownStart < COOLDOWN_TIME) {
                    getNeighborNode(graphDb,
                        modGet(neighborIndices, i),
                        modGet(nodeAttributes, i),
                        modGet(nodeQueries, i));
                    ++i;
                }
                out.printf("%d %d\n", (int) queryThput, (int) edgesThput);

            } catch (Exception e) {
                System.err.printf("Client %d throughput bench exception: %s\n",
                    clientId, e);
                System.exit(1);
            } finally {
                if (out != null) {
                    out.close();
                }
                tx.success();
                tx.close();
            }
        }
    }

    private static void neighborNodeThroughput(
        boolean tuned, String dbPath, String neo4jPageCacheMem, int numClients) {

        GraphDatabaseService graphDb;
        System.out.println("About to open database");
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
        System.out.println("Done opening");

        BenchUtils.registerShutdownHook(graphDb);
        Transaction tx = null;
        try {
            tx = graphDb.beginTx();
            if (tuned) {
                BenchUtils.fullWarmup(graphDb);
            }
        } finally {
            if (tx != null) {
                tx.success();
                tx.close();
            }
        }

        try {
            List<Thread> clients = new ArrayList<>(numClients);
            for (int i = 0; i < numClients; ++i) {
                clients.add(new Thread(new RunNeighborNodeThroughput(
                    i, graphDb)));
            }
            for (Thread thread : clients) {
                thread.start();
            }
            for (Thread thread : clients) {
                thread.join();
            }
        } catch (Exception e) {
            System.err.printf("Benchmark throughput exception: %s\n", e);
            System.exit(1);
        } finally {
            BenchUtils.printMemoryFootprint();
            System.out.println("Shutting down database ...");
            graphDb.shutdown();
        }
    }

    private static void neighborNodeLatency(boolean tuned,
        String dbPath, String neo4jPageCacheMem, String outputFile,
        boolean useIndex) {

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

            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
                outputFile)));
            PrintWriter resOut = null;
            if (System.getenv("BENCH_PRINT_RESULTS") != null) {
                resOut = new PrintWriter(new BufferedWriter(
                    new FileWriter(outputFile + ".neo4j_result")));
            }

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
            out.close();
            if (resOut != null) {
                resOut.flush();
                resOut.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
