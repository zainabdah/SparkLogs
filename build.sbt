ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkLogs"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-hadoop
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "6.4.1"
