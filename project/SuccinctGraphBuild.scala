import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

object SuccinctGraphBuild extends Build {

  lazy val root = project.in(file("."))
    .dependsOn(benchmark)
    .settings(assemblySettings: _*)
    .settings(commonSettings: _*)
    .settings(TestSettings.settings: _*)

  lazy val benchmark = project.in(file("benchmark"))
    .settings(assemblySettings: _*)
    .settings(commonSettings: _*)
    .settings(TestSettings.settings: _*)
    .settings(name := "succinctgraph-benchmark")

  lazy val commonSettings = Seq(
    name := "succinctgraph",
    version := "0.1.0-SNAPSHOT",
    organization := "edu.berkeley.cs",

    resolvers ++= Seq(
     "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
      "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
      "Spray" at "http://repo.spray.cc"
    ),

    test in assembly := {},

    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
      {
        case PathList("javax", "servlet", xs @ _*)           => MergeStrategy.first
        case PathList(ps @ _*) if ps.last endsWith ".html"   => MergeStrategy.first
        case "application.conf"                              => MergeStrategy.concat
        case "reference.conf"                                => MergeStrategy.concat
        case "log4j.properties"                              => MergeStrategy.discard
        case m if m.toLowerCase.endsWith("manifest.mf")      => MergeStrategy.discard
        case m if m.toLowerCase.matches("meta-inf.*\\.sf$")  => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    }
  )

}

object TestSettings {
  lazy val settings = Seq(
    fork := true,
    parallelExecution in Test := false
  )
}
