import sbt._
import Keys._

libraryDependencies ++= Seq(
  "org.neo4j" % "neo4j" % "2.2.2"
)

{
  val defaultSparkVersion = "1.5.0"
  val sparkVersion =
    scala.util.Properties.envOrElse("SPARK_VERSION", defaultSparkVersion)
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.10" % sparkVersion excludeAll(excludeHadoop)
  )
}
