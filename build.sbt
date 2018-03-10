name := "spark-structured-streaming"

version := "0.2"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "6.2.2"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.0"