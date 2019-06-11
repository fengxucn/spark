package com.fx.demo.streaming

import com.fx.demo.utils.SparkUtils._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.LogManager
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, StreamingContext}

object streamingDemo {
  def main(args: Array[String]): Unit = {
    streaming(args)
  }


  private def streaming(args: Array[String]): Unit = {
    var ssc: StreamingContext = null
    val (properties, log) = loadProperties(args)
    val config = Map("spark.streaming.backpressure.initialRate" -> "0.1",
      "spark.streaming.backpressure.enabled" -> "true",
      "spark.streaming.receiver.maxRate" -> "1000", //Maximum rate (number of records per second) at which each receiver will receive data
      "spark.streaming.kafka.maxRatePerPartition" -> "1000", //Maximum rate (number of records per second) at which data will be read from each Kafka partition when using the new Kafka direct stream API
      "spark.executor.instances" -> "6",
      "spark.executor.cores" -> "4",
      "spark.yarn.maxAppAttempts" -> "4",
      "spark.yarn.am.attemptFailuresValidityInterval" -> "1h",
      "spark.yarn.executor.failuresValidityInterval" -> "1h",
      "spark.yarn.max.executor.failures" -> "4",
      "spark.task.maxFailures" -> "4",
      "spark.streaming.kafka.consumer.poll.ms" -> "60000",
      "spark.blacklist.task.maxTaskAttemptsPerExecutor" -> "2",
      "spark.blacklist.task.maxTaskAttemptsPerNode" -> "2",
      "spark.blacklist.stage.maxFailedTasksPerExecutor" -> "4",
      "spark.blacklist.stage.maxFailedExecutorsPerNode" -> "4"
    )
    val (spark, logMsg) = buildSparkSession(properties, "castar_streaming", config)
    try {
      val duration = properties.getProperty("Duration", "60").toInt
      ssc = new StreamingContext(spark.sparkContext, Minutes(duration.toLong))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> properties.getProperty("BOOTSTRAP_SERVERS_CONFIG"),
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> properties.getProperty("GROUPID"),
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "heartbeat.interval.ms" -> (10000: java.lang.Integer),
        "session.timeout.ms" -> (((duration * 60 * 1000) + 5000): java.lang.Integer),
        "receive.buffer.bytes" -> (565536000: java.lang.Integer),
        "request.timeout.ms" -> (((duration * 60 * 1000) + 10000): java.lang.Integer)
      )
      val topics = properties.getProperty("TOPIC").split(",").toSet

      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

      import spark.implicits._
      stream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        rdd.flatMap(record => {
          //message to DF
          record.value().split(",")
        }).toDF()
          .repartition($"day", $"type")
          .write
          .partitionBy("day", "type")
          .format(properties.getProperty("fileFormat"))
          .option("header", "true")
          .option("codec", properties.getProperty("sparkCodeC"))
          .mode("append")
          .saveAsTable(properties.getProperty("hive"))

        log.info("save success.")

        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        log.info("offset commit success.")
      }
      ssc.start()

      ssc.awaitTermination()

    } catch {
      case exception: Throwable => {
        val log = LogManager.getRootLogger
        log.error("Exception happens", exception)
        //TODO
        ssc.stop()
        throw exception
      }
    }
  }
}
