package com.tikal.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingAggregation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("streaming-test")
      .master("local[*]")
      .getOrCreate()

    //Define schema
    val schema = StructType(Seq(
      StructField("batch", StringType, nullable = false),
      StructField("key", StringType, nullable = false),
      StructField("time", TimestampType, nullable = false),
      StructField("amount", LongType, nullable = false)
    ))

    //Read CSV source from directory
    val files = spark.readStream
      .schema(schema)
      .csv("stream-input")

    //Perform aggregation
    val grouped = files
      .withWatermark("time", "5 seconds")
      .groupBy(window(col("time"), "30 seconds"))
        .agg(
          count("*").alias("count"),
          avg("amount").alias("avgAmount")
        )

    //Stream output to console
    val query = grouped.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()

    //Wait forever
    query.awaitTermination()
  }
}
