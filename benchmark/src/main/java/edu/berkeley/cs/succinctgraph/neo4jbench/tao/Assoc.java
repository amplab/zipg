package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import org.neo4j.graphdb.Relationship;

public class Assoc {
    public long srcId, dstId, atype;
    public long timestamp;
    public String attr; // For now assumes one edge attribute

    public Assoc(
        long srcId, long dstId, long atype, long timestamp, String attr) {

        this.srcId = srcId;
        this.dstId = dstId;
        this.atype = atype;
        this.timestamp = timestamp;
        this.attr = attr;
    }

    /** Ctor that brings relevant data into memory. */
    public Assoc(Relationship rel) {
        this.srcId = rel.getStartNode().getId();
        this.dstId = rel.getEndNode().getId();
        this.atype = Long.valueOf(rel.getType().name());
        this.timestamp = (long) (rel.getProperty("timestamp")); // hard-coded
        // Empty string to guard against empty attribute
        this.attr = (String) (rel.getProperty("attr", "")); // hard-coded
    }

    public String toString() {
        return String.format(
            "Assoc[src=%d, dst=%d, atype=%d, time=%d, attr '%s']",
            srcId, dstId, atype, timestamp, attr);
    }

}
