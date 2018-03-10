package com.tikal.spark.tests

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SparkReadKafka {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("streaming-test")
      .master("local[*]")
      .getOrCreate()

    val kafkaDf: Dataset[Row] = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "stream1")
      .load()

    kafkaDf.show()

    //Define schema
    val schema = StructType(Seq(
      StructField("batch", StringType, nullable = false),
      StructField("key", StringType, nullable = false),
      StructField("time", TimestampType, nullable = false),
      StructField("amount", LongType, nullable = false)
    ))

    val parsedDf = kafkaDf
      .select(from_json(col("value").cast("string"), schema).alias("fields"), col("offset"))
      .select("fields.*")

    parsedDf.show()
  }

}
