// === Project coordinates & Scala pin ===
ThisBuild / organization := "com.example"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.19"

// === Java 11 (per syllabus) ===
ThisBuild / javacOptions ++= Seq("--release", "11")
ThisBuild / scalacOptions ++= Seq(
  "-deprecation", "-feature", "-unchecked", "-Xlint",
  "-Ywarn-dead-code", "-Ywarn-numeric-widen"
)

// === Versions ===
lazy val sparkVersion  = "3.5.1"
lazy val hadoopVersion = "3.3.5"

// === Dependencies ===
// Keep Spark not `provided` for local tests. For cluster builds, switch to % "provided".
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion,
  "org.apache.spark" %% "spark-sql"     % sparkVersion,
  "org.scala-lang"    % "scala-reflect" % scalaVersion.value,
  "org.scalatest"    %% "scalatest"     % "3.2.19" % Test,

  // Lock Hadoop to avoid transitive conflicts in local/Windows environments
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api"     % hadoopVersion
)

ThisBuild / dependencyOverrides ++= Seq(
  "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client-api"     % hadoopVersion
)

// === Test runtime stability for Spark ===
Test / parallelExecution := false       // Do not run suites in parallel
Test / fork              := true        // Run each suite in a separate JVM (Spark-friendly)
Compile / run / fork     := true        // Run runMain in a separate JVM as well


