package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import org.neo4j.graphdb.RelationshipType;

public class AType implements RelationshipType {
    String name_;

    public AType(String name) {
        this.name_ = name;
    }

    public String name() {
        return this.name_;
    }
}
