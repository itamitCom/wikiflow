package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._

object DeltaReadConsumer extends App with LazyLogging {
  val appName: String = "delta-read-consumer"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Delta Read consumer")

  /**
    * Advanced:
    * - попробуйте написать consumer-читатель из папки в которую пишет AnalyticsConsumer, который просто будет выводить его на консоль.
    * - попробуйте добавить sliding-window который агрегирует подсчет
    */

  val inputStream = spark
    .readStream
    .format("delta")
    .load("/storage/analytics-consumer/output")

  val consoleOutput = inputStream
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}
