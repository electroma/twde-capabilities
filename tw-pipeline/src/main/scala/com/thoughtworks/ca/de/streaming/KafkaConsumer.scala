package com.thoughtworks.ca.de.streaming

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val log = LogManager.getRootLogger
    val spark = SparkSession.builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._

    /*
    Writer code here to:
    +1. read citybikes topic stream from kafka
    +2. Convert kafka JSON payload to dataframe rows
    +3. Convert start time and stop time columns to Timestamp
    +4. Convert birth year column to Date
    5. Count number of rides per minute window
    6. Write to file system/hdfs in parquet format partitioned by minute window
     */


    val fields = Array(
      "tripduration", "starttime", "stoptime", "start station id",
      "start station name", "start station latitude", "start station longitude",
      "end station id", "end station name", "end station latitude", "end station longitude",
      "bikeid", "usertype", "birth year", "gender")
      .map(a => DataTypes.createStructField(a, DataTypes.StringType, false))

    val schema = DataTypes.createStructType(fields)

    val bikesDF = spark.readStream
      .format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> conf.getString("streaming.kafka.hosts"),
        "subscribe" -> conf.getString("streaming.kafka.topic"),
        "startingOffsets" -> "earliest"))
      .load()
      .selectExpr("CAST(value AS STRING) as message")
      .select(functions.from_json(functions.col("message"), schema).as("json"))
      .select("json.*")
      .withColumn("starttime", $"starttime".cast(TimestampType))
      .withColumn("stoptime", $"stoptime".cast(TimestampType))
      .withColumn("birth year", $"birth year".cast(DateType))

    bikesDF.printSchema()
  }
}
