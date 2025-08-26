// Pin Scala per syllabus (and to match Spark 3.5.x artifacts)
ThisBuild / scalaVersion := "2.12.19"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.example"

lazy val sparkVersion  = "3.5.1"
lazy val hadoopVersion = "3.3.5"

// Spark + testing dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion,
  "org.apache.spark" %% "spark-sql"     % sparkVersion,
  "org.scala-lang"    % "scala-reflect" % scalaVersion.value,
  "org.scalatest"    %% "scalatest"     % "3.2.19" % Test
)

// Add Hadoop 3.3.5 and enforce this version across the build
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api"     % hadoopVersion
)

ThisBuild / dependencyOverrides ++= Seq(
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api"     % hadoopVersion
)


