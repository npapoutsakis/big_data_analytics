ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "INF424_Spark_Analytics_2024"
  )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.5.1",
    "org.apache.spark" %% "spark-sql" % "3.5.1"
)