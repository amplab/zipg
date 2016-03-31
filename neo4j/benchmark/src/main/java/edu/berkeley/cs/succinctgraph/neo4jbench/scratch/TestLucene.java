package edu.berkeley.cs.succinctgraph.neo4jbench.scratch;

import org.apache.lucene.store.FSDirectory;

import java.io.File;

public class TestLucene {

    public static void main(String[] args) {
        try {
            FSDirectory dirOfIndex = FSDirectory.open(new File(args[0]));
            System.out.printf("Class name: %s\n", dirOfIndex.getClass().getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
