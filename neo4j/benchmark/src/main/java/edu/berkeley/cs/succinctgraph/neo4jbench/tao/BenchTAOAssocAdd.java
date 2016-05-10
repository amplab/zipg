package edu.berkeley.cs.succinctgraph.neo4jbench.tao;

import edu.berkeley.cs.succinctgraph.neo4jbench.BenchUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class BenchTAOAssocAdd {
  final static long SEED = 2331L;
  final static TAOImpls taoImpls = new TAOImpls();
  static long NUM_NODES;
  static Random warmupRand;
  static Random rand;
  public static List<String> EDGE_ATTR;

  static {
    char[] chars = new char[128];
    Arrays.fill(chars, '|');
    EDGE_ATTR = new ArrayList<>();
    EDGE_ATTR.add(new String(chars));
  }

  static int numWarmupQueries, numMeasureQueries;

  public static void main(String[] args) {
    String type = args[0];
    String dbPath = args[1];
    String outputFile = args[2];
    NUM_NODES = Long.parseLong(args[3]);
    boolean tuned = Boolean.valueOf(args[4]);
    String neo4jPageCacheMemory = args[5];
    numWarmupQueries = 1000;
    numMeasureQueries = 10000;
    warmupRand = new Random(SEED);
    rand = new Random(SEED + 1);

    if (type.equals("latency")) {
      benchAssocCountLatency(tuned, dbPath, neo4jPageCacheMemory, outputFile);
    } else {
      System.err.println("Unknown type: " + type);
    }
  }

  private static void benchAssocCountLatency(boolean tuned, String dbPath, String neo4jPageCacheMem,
    String outputFile) {

    System.out.println("Benchmarking assoc_add() queries");

    GraphDatabaseService db;
    System.out.println("About to open database");
    if (tuned) {
      db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath)
        .setConfig(GraphDatabaseSettings.cache_type, "none")
        .setConfig(GraphDatabaseSettings.pagecache_memory, neo4jPageCacheMem).newGraphDatabase();
    } else {
      db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
    }
    System.out.println("Done opening");

    BenchUtils.registerShutdownHook(db);
    Transaction tx;
    try {
      BenchUtils.fullWarmup(db);

      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));

      PrintWriter resOut = null;
      if (System.getenv("BENCH_PRINT_RESULTS") != null) {
        resOut = new PrintWriter(new BufferedWriter(new FileWriter(outputFile + ".neo4j_result")));
      }

      System.out.println("Warming up for " + numWarmupQueries + " queries");
      for (int i = 0; i < numWarmupQueries; ++i) {
        long id1 = Math.abs(warmupRand.nextLong()) % NUM_NODES;
        long id2 = Math.abs(warmupRand.nextLong()) % NUM_NODES;
        int atype = warmupRand.nextInt(5);
        tx = db.beginTx();
        taoImpls.assocAdd(db, id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
        tx.success();
        tx.close();
      }

      System.out.println("Measuring for " + numMeasureQueries + " queries");
      for (int i = 0; i < numMeasureQueries; ++i) {
        long id1 = Math.abs(rand.nextLong()) % NUM_NODES;
        long id2 = Math.abs(rand.nextLong()) % NUM_NODES;
        int atype = rand.nextInt(5);
        long queryStart = System.nanoTime();
        tx = db.beginTx();
        taoImpls.assocAdd(db, id1, id2, atype, System.currentTimeMillis(), EDGE_ATTR);
        tx.success();
        tx.close();
        long queryEnd = System.nanoTime();
        double microsecs = (queryEnd - queryStart) / ((double) 1000);
        out.println("1," + microsecs);

        if (resOut != null) {
          resOut.printf("%d %d %d\n", id1, id2, atype);
        }
      }
      out.close();
      if (resOut != null) {
        resOut.flush();
        resOut.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      System.out.println("Shutting down database ...");
      db.shutdown();
    }
  }
}
