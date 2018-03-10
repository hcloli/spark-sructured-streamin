package com.tikal.spark.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object SparkStreamingKafkaToElasticsearch {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("streaming-test")
      .master("local[*]")
      .getOrCreate()

    //Read from kafka topic
    val kafkaDf: Dataset[Row] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest") //Only for demo
      .option("subscribe", "stream1")
      .load()

    //Define schema for JSON conversion
    val schema = StructType(Seq(
      StructField("batch", StringType, nullable = false),
      StructField("key", StringType, nullable = false),
      StructField("time", TimestampType, nullable = false),
      StructField("amount", LongType, nullable = false),
      StructField("month", LongType, nullable = false)
    ))

    //Convert the "value" column, which is the message body, to columns
    val parsedDf = kafkaDf
      .select(from_json(col("value").cast("string"), schema).alias("fields"), col("offset"))
      .select("fields.*")

    //Do some transformation
    val transformed = parsedDf.withColumn("year", year(col("time")))

    //Write stream to elastic
    val query = transformed
      .writeStream
      .outputMode("append")
      .format("es")
      .option(ConfigurationOptions.ES_MAPPING_ID, "key") //Make the output idempotent
      .option(ConfigurationOptions.ES_NODES, "127.0.0.1")
      .option(ConfigurationOptions.ES_PORT, "9200")
      .option("checkpointLocation", "stream-elastic-checkpoint")
      .start("stream1/doc") //Name of index and doc type

    //Wait forever
    query.awaitTermination()
  }

}
