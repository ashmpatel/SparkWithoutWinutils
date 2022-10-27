
ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / name             := "seed-spark"
ThisBuild / organization     := "com.dataintoresults"

lazy val seedSpark = (project in file("."))
  .settings(
    name := "seed-spark",
    libraryDependencies := Seq(
	"org.apache.spark" %% "spark-core" % "2.4.0",
	"org.apache.spark" %% "spark-sql" % "2.4.0",
	"com.typesafe" % "config" % "1.2.1",
	"org.xerial.snappy" % "snappy-java" % "1.0.4.1"
	) 
  )
