name := "SparkJobServer"

version := "0.1.0"

scalaVersion := "2.10.5"

val Spark = "1.4.1"
val SparkCassandra = "1.4.1"
val SparkJobServer = "0.5.2"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq(
  "spark.jobserver" %% "job-server-api" % "0.5.2" % "provided" withSources() withJavadoc(),
  "spark.jobserver" %% "job-server-extras" % "0.5.2" % "provided" withSources() withJavadoc(),
  "org.apache.spark" % "spark-core_2.10" % Spark % "provided" withSources() withJavadoc(),
  "org.apache.spark"  %% "spark-sql" % Spark % "provided" withSources() withJavadoc(),
  "org.apache.spark"  %% "spark-hive" % Spark % "provided" withSources() withJavadoc(),
  "com.datastax.spark" %% "spark-cassandra-connector" % SparkCassandra % "provided" withSources() withJavadoc()
)
