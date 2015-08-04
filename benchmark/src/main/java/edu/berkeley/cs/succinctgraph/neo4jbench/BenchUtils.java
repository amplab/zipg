package edu.berkeley.cs.succinctgraph.neo4jbench;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class BenchUtils {

    public static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    public static void awaitIndexes(GraphDatabaseService graphDb) {
        Schema schema = graphDb.schema();
        try {
            schema.awaitIndexesOnline(60, TimeUnit.SECONDS);
            System.out.println("Indexes online");
        } catch (IllegalStateException e) {
            System.err.println("Indexes not all online after 60 seconds: " + e.getMessage());
            throw e;
        }
    }

    public static <T> T modGet(List<T> xs, int i) {
        return xs.get(i % xs.size());
    }

    public static <T> void print(String header, List<T> xs, PrintWriter out) {
        out.println(header);
        for (T x : xs) {
            out.printf("%s ", x);
        }
        out.println();
    }

    public static void printMemoryFootprint() {
        Runtime rt = Runtime.getRuntime();
        long max = rt.maxMemory();
        long allocated = rt.totalMemory();
        System.out.printf(
            "JVM memory: Max %.1f GB, Allocated %.1f GB, Used %.1f GB\n",
            max * 1. / (1L << 30),
            allocated * 1. / (1L << 30),
            (allocated - rt.freeMemory()) * 1. / (1L << 30));
    }

    /** Scans through all data including edge properties and node properties. */
    public static void fullWarmup(GraphDatabaseService graphDb) {
        long start = System.nanoTime();
        Iterable<Relationship> iter = GlobalGraphOperations.at(graphDb)
            .getAllRelationships();
        for (Relationship rel : iter) {
            rel.getId();
            rel.getType();
            rel.getStartNode();
            rel.getEndNode();
            rel.getPropertyKeys();
        }
        ResourceIterable<Node> nodes = GlobalGraphOperations.at(graphDb)
            .getAllNodes();
        for (Node node : nodes) {
            node.getId();
            node.getRelationshipTypes();
            node.getRelationships();
            node.getPropertyKeys();
        }
        long end = System.nanoTime();
        printMemoryFootprint();
        System.out.println(
            "Full warmup done in " + (end - start) / 1e6 + " millis");
    }

    static class TimestampedId implements Comparable<TimestampedId> {
        public long timestamp = -1, id = -1;
        public TimestampedId(long timestamp, long id) {
            this.timestamp = timestamp;
            this.id = id;
        }
        // Larger timestamp comes first.
        public int compareTo(TimestampedId that) {
            long diff = that.timestamp - this.timestamp;
            if (diff == 0) return 0;
            return diff > 0 ? 1 : -1;
        }
    }

    public static void getNodeQueries(
        String file, List<Integer> indices1, List<Integer> indices2,
        List<String> queries1, List<String> queries2) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] tokens = line.split("\\x02");
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

    public static void getNeighborQueries(String file, List<Long> nhbrs) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            List<String> lines = new ArrayList<String>();
            String line = br.readLine();
            while (line != null) {
                lines.add(line);
                line = br.readLine();
            }
            for (int i = 0; i < lines.size(); i++) {
                nhbrs.add(Long.parseLong(lines.get(i)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getNeighborAtypeQueries(
        String file, List<Long> nodeIds, List<Long> atypes) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                String[] toks = line.split(",");
                nodeIds.add(Long.valueOf(toks[0]));
                atypes.add(Long.valueOf(toks[1]));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getNeighborNodeQueries(
        String file, List<Long> indices,
        List<Integer> attributes, List<String> queries) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                indices.add(Long.parseLong(line.substring(0, idx)));
                int idx2 = line.indexOf(',', idx + 1);
                attributes.add(Integer.parseInt(line.substring(idx + 1, idx2)));
                queries.add(line.substring(idx2 + 1));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAssocRangeQueries(
        String file, List<Long> nodes, List<Long> atypes,
        List<Integer> offsets, List<Integer> lengths) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Long.parseLong(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                offsets.add(Integer.parseInt(line.substring(idx2 + 1, idx3)));

                lengths.add(Integer.parseInt(line.substring(idx3 + 1)));
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAssocGetQueries(
        String file, List<Long> nodes, List<Long> atypes,
        List<Set<Long>> dstIdSets, List<Long> tLows, List<Long> tHighs) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Long.parseLong(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                tLows.add(Long.parseLong(line.substring(idx2 + 1, idx3)));

                int idx4 = line.indexOf(',', idx3 + 1);
                tHighs.add(Long.parseLong(line.substring(idx3 + 1, idx4)));

                int idxLast = idx4, idxCurr;
                Set<Long> dstIdSet = new HashSet<>();
                while (true) {
                    idxCurr = line.indexOf(',', idxLast + 1);
                    if (idxCurr == -1) {
                        break;
                    }
                    dstIdSet.add(Long.parseLong(
                        line.substring(idxLast + 1, idxCurr)));
                    idxLast = idxCurr;
                }
                dstIdSet.add(Long.parseLong(line.substring(idxLast + 1)));
                dstIdSets.add(dstIdSet);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAssocTimeRangeQueries(
        String file, List<Long> nodes, List<Long> atypes,
        List<Long> tLows, List<Long> tHighs, List<Integer> limits) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                int idx = line.indexOf(',');
                nodes.add(Long.parseLong(line.substring(0, idx)));

                int idx2 = line.indexOf(',', idx + 1);
                atypes.add(Long.parseLong(line.substring(idx + 1, idx2)));

                int idx3 = line.indexOf(',', idx2 + 1);
                tLows.add(Long.parseLong(line.substring(idx2 + 1, idx3)));

                int idx4 = line.indexOf(',', idx3 + 1);
                tHighs.add(Long.parseLong(line.substring(idx3 + 1, idx4)));

                limits.add(Integer.parseInt(line.substring(idx4 + 1)));

                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
