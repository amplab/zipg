package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.lucene.LuceneTimeline;
import org.neo4j.index.lucene.TimelineIndex;

import java.util.*;

public class TAOImpls implements TAOIface {

    // Read workload distribution; from ATC 13 Bronson et al.
    final static double ASSOC_RANGE_PERC = 0.409;
    final static double OBJ_GET_PERC = 0.289;
    final static double ASSOC_GET_PERC = 0.157;
    final static double ASSOC_COUNT_PERC = 0.117;
    final static double ASSOC_TIME_RANGE_PERC = 0.028;

    private static int MAX_NUM_ATYPES = 1618;
    private static RelationshipType[] atypeMap;
    private static Comparator<Assoc> sortAssocByDescendingTime;
    static {
        System.out.println(
            "Neo4j's TAO impls assume max # atypes = " + MAX_NUM_ATYPES);

        atypeMap = new RelationshipType[MAX_NUM_ATYPES];
        // Assumes atypes are consecutive ints in range [0, maxATypes).
        for (int i = 0; i < MAX_NUM_ATYPES; ++i) {
            atypeMap[i] = new AType(String.valueOf(i));
        }

        sortAssocByDescendingTime = new Comparator<Assoc>() {
            @Override
            public int compare(Assoc o1, Assoc o2) {
                if (o2.timestamp == o1.timestamp) {
                    return 0;
                }
                return o1.timestamp < o2.timestamp ? 1 : -1;
            }
        };
    }

    public static int chooseQuery(Random rand) {
        double d = rand.nextDouble();
        if (d < ASSOC_RANGE_PERC) {
            return 0;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC) {
            return 1;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC + ASSOC_GET_PERC) {
            return 2;
        } else if (d < ASSOC_RANGE_PERC + OBJ_GET_PERC +
            ASSOC_GET_PERC + ASSOC_COUNT_PERC) {

            return 3;
        }
        return 4;
    }

    // TODO: note that using an index on *all edges* doesn't make sense.
    // Refs:
    // http://neo4j.com/docs/stable/indexing-lucene-extras.html
    // http://neo4j.com/docs/stable/indexing-create-advanced.html
    /**
     * Uses Neo4j's "legacy indexing" feature to create an index on the edge
     * timestamps.  This is a funny name to give to something that's not really
     * legacy -- their newer "schema indexes" do not support some important
     * features such as edge indexes or range queries.
     */
    public static void indexRelsOnTimestamps(GraphDatabaseService graphDb) {
        IndexManager index = graphDb.index();
        RelationshipIndex relIndex = index.forRelationships("timestamps");
        BenchUtils.awaitIndexes(graphDb);
        assert(index.existsForRelationships("timestamps"));

        TimelineIndex<Relationship> timeline =
            new LuceneTimeline<Relationship>(graphDb, relIndex);
        timeline.getBetween(null, null);
    }

    // TODO: assumes keys are in-order? (name0, name1, ..)
    public List<String> objGet(GraphDatabaseService db, long nodeId) {
        Node n = db.getNodeById(nodeId);
        List<String> res = new ArrayList<>();
        for (String key : n.getPropertyKeys()) {
            res.add((String) (n.getProperty(key)));
        }
        return res;
    }

    public void objAdd(GraphDatabaseService db, List<String> attributes) {
        Node node = db.createNode();
        for (int i = 0; i < attributes.size(); i++) {
            node.setProperty("name" + i, attributes.get(i));
        }
    }

    public void assocAdd(GraphDatabaseService db, long src, long dst, int atype, long timestamp,
      List<String> attributes) {
        Node n1 = db.getNodeById(src);
        Node n2 = db.getNodeById(dst);
        Relationship rel = n1.createRelationshipTo(n2, atypeMap[atype]);
        rel.setProperty("timestamp", timestamp);
        for (int i = 0; i < attributes.size(); i++) {
            rel.setProperty("name" + i, attributes.get(i));
        }
    }

    /** Scans over all the assoc list and manually sorts by timestamp. */
    public List<Assoc> assocRange(GraphDatabaseService db,
        long src, long atype, int off, int len) {

        Node n = db.getNodeById(src);
        Iterable<Relationship> rels =
            n.getRelationships(atypeMap[(int) atype], Direction.OUTGOING);
        List<Assoc> assocs = new ArrayList<>();
        for (Relationship rel : rels) {
            assocs.add(new Assoc(rel));
        }
        if (off < 0 || off >= assocs.size()) {
            return Collections.emptyList();
        }
        Collections.sort(assocs, sortAssocByDescendingTime);
        return assocs.subList(off, Math.min(assocs.size(), off + len));
    }

    /** Scans (filters on the fly) the assoc list.  Sorts the filtered result.
     * Lastly do 2 binary searches and return the in-range sublist. */
    public List<Assoc> assocGet(GraphDatabaseService db,
        long src, long atype, Set<Long> dstIdSet, long low, long high) {

        Node n = db.getNodeById(src);
        Iterable<Relationship> rels =
            n.getRelationships(atypeMap[(int) atype], Direction.OUTGOING);
        List<Assoc> assocs = new ArrayList<>();

        // Why we don't use binary searches:
        // Since timestamp is just a user-level property, accessing it in
        // the binary searches will nonetheless load all properties of that rel
        // into memory.  Hence, we could've just piggyback an additional filter
        // on timestamp here.
        Assoc assoc;
        for (Relationship rel : rels) {
            if (dstIdSet.contains(rel.getEndNode().getId())) {
                assoc = new Assoc(rel);
                if (assoc.timestamp >= low && assoc.timestamp <= high) {
                    assocs.add(assoc);
                }
            }
        }
        Collections.sort(assocs, sortAssocByDescendingTime);
        return assocs;
    }

    /** Scans and counts. */
    public long assocCount(GraphDatabaseService db, long src, long atype) {
        Node n = db.getNodeById(src);
        Iterable<Relationship> rels =
            n.getRelationships(atypeMap[(int) atype], Direction.OUTGOING);
        long count = 0;
        for (Relationship _ : rels) {
            ++count;
        }
        return count;
    }

    /** The implementation is basically the same as assocGet(). */
    public List<Assoc> assocTimeRange(GraphDatabaseService db,
        long src, long atype, long low, long high, int limit) {

        Node n = db.getNodeById(src);
        Iterable<Relationship> rels =
            n.getRelationships(atypeMap[(int) atype], Direction.OUTGOING);
        List<Assoc> assocs = new ArrayList<>();

        // Why we don't use binary searches:
        // Since timestamp is just a user-level property, accessing it in
        // the binary searches will nonetheless load all properties of that rel
        // into memory.  Hence, we could've just piggyback an additional filter
        // on timestamp here.
        Assoc assoc;
        for (Relationship rel : rels) {
            assoc = new Assoc(rel);
            if (assoc.timestamp >= low && assoc.timestamp <= high) {
                assocs.add(assoc);
            }
        }
        Collections.sort(assocs, sortAssocByDescendingTime);
        return assocs.subList(0, Math.min(limit, assocs.size()));
    }

}
