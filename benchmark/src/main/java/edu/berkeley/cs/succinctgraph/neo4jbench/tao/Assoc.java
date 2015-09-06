package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import org.neo4j.graphdb.Relationship;

public class Assoc {
    public long srcId, dstId, atype;
    public long timestamp;
    public String attr = null; // For now assumes one edge attribute

    private Relationship relSaved;

    /** Ctor that brings relevant data into memory. */
    public Assoc(Relationship rel) {
        this.srcId = rel.getStartNode().getId();
        this.dstId = rel.getEndNode().getId();
        this.atype = Long.valueOf(rel.getType().name());
        this.timestamp = (long) (rel.getProperty("timestamp")); // hard-coded
        this.relSaved = rel;
    }

    public void loadAttr() {
        // Empty string to guard against empty attribute
        this.attr = (String) (relSaved.getProperty("attr", "")); // hard-coded
    }

    public String toString() {
        if (attr == null) {
            loadAttr();
        }
        return String.format(
            "[src=%d,dst=%d,atype=%d,time=%d,attr='%s']",
            srcId, dstId, atype, timestamp, attr);
    }

}
