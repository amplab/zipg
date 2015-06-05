package edu.berkeley.cs.succinctgraph.neo4jbench

import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.tooling.GlobalGraphOperations

import java.util.{HashSet => JHashSet}

import scala.collection.JavaConverters._

object CheckGraph {

  def setDiff(set1: JHashSet[(Int, Int)], set2: JHashSet[(Int, Int)]): String = {
    val setA = if (set1.size() < set2.size()) set1 else set2
    val setB = if (set1.size() < set2.size()) set2 else set1
    val sb = new StringBuilder()
    setB.iterator().asScala.foreach { case edge =>
      if (!setA.contains(edge)) sb.append(edge).append("; ")
    }
    sb.toString()
  }

  def checkEdges(neo4jPath: String, graphPath: String) = {
    val edges = buildEdgesFromNeo4j(neo4jPath)
    val graphEdges = new JHashSet[(Int, Int)]()

    var numRelationships = 0
    scala.io.Source.fromFile(graphPath).getLines().foreach { line =>
      val splits = line.split(" ")
      val edge = (splits(0).toInt, splits(1).toInt)
      if (!edges.contains(edge)) {
        sys.error(s"neo4j does not contain edge $edge!")
      }
      if (!graphEdges.contains(edge)) {
        numRelationships += 1
        graphEdges.add(edge)
      }
    }
    if (numRelationships != edges.size()) {
      println(s".edge has $numRelationships unique edges, but neo4j has ${edges.size()}")
      println(s"Larger - Smaller: ${setDiff(edges, graphEdges)}")
    } else {
      println("OK: Edges are the same (ignoring node properties).")
    }
  }

  def buildEdgesFromNeo4j(neo4jPath: String): JHashSet[(Int, Int)] = {
    val graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(neo4jPath)
    BenchUtils.registerShutdownHook(graphDb)

    val tx = graphDb.beginTx()
    try {
      val allRels = GlobalGraphOperations
        .at(graphDb)
        .getAllRelationships
        .asScala

      val edgeTable = new JHashSet[(Int, Int)]()

      for (relationship <- allRels) {
        val v = relationship.getStartNode.getId.toInt
        val w = relationship.getEndNode.getId.toInt
        edgeTable.add((v, w))
      }

      edgeTable
    } finally {
      tx.close()
    }
  }

  def main(args: Array[String]) {
    val neo4jPath = args(0)
    val graphPath = args(1)
    println(s"neo4j path: $neo4jPath")
    println(s".edge path: $graphPath")
    checkEdges(neo4jPath, graphPath)
  }

}
