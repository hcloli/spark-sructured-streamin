package com.tikal.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object SparkStreamingBasic {

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

    //Make some transformation
    val transformed = files.withColumn("month", month(col("time")))

    //Write output to stream
    val query = transformed
        .writeStream
        .outputMode("append") //Can be one of "complete", "update" or "append"
        .format("console")
        .option("truncate", "false")
        .option("numRows", "200")
        .start()

    //Wait forever
    query.awaitTermination()

  }

}
