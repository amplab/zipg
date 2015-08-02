package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Set;

public interface TAOIface {

    /** Returns all attributes for a node, as a single String. */
    public String nodeGet(GraphDatabaseService db, long nodeId);

    public List<Assoc> assocRange(
        GraphDatabaseService db, long src, long atype, int off, int len);

    public List<Assoc> assocGet(GraphDatabaseService db,
        long src, long atype, Set<Long> dstIdSet, long low, long high);

    public long assocCount(GraphDatabaseService db, long src, long atype);

    public List<Assoc> assocTimeRange(GraphDatabaseService db,
        long src, long atype, long low, long high, int limit);

}
