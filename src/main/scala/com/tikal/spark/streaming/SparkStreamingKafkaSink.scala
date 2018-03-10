package com.tikal.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingKafkaSink {

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

    //Prepare DS for kafka. Dataset must include "value" column (for message body) and may include "key" column
    val dsToKafka = transformed
      .select(col("batch").alias("key"),
              to_json(struct(col("*"))).alias("value"))


    //Write stream to kafka topic
    val query = dsToKafka
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "stream1")
      .option("checkpointLocation", "stream-kafka-checkpoint")
      .start()

    //Wait forever
    query.awaitTermination()

  }
}
