name := "Yelp"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

val json4sNative = "org.json4s" %% "json4s-native" % "3.6.4"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scala-lang.modules" %% "scala-xml" % "1.1.1",
  json4sNative
)