package com.fx.demo.utils

import java.lang.management.ManagementFactory
import java.util.Properties

import com.datastax.spark.connector.rdd.ReadConf
import org.apache.commons.cli.{BasicParser, CommandLine, Options}
import org.apache.log4j.{LogManager, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._

object SparkUtils {
  val LOG4J_CONF    = "/log4j-local.properties"
  val PARMS         = "/parms-local-csv.properties"
  val ideName = List("Eclipse", "IntelliJ", "Idea")
  val ide: Boolean = ideName.exists(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains)
  val DATE_FORMAT = "MM/dd/yyyy"


  def loadProperties(args: Array[String]): (Properties, Logger) = {
    val cmd = getParser(args)

    val log = {
      if (cmd.hasOption("log4j"))
        getLog(cmd.getOptionValue("log4j"))
      else
        getLog(LOG4J_CONF)
    }

    val properties = {
      if (cmd.hasOption("configLocation")) {
        log.warn("Using config Location : " + cmd.getOptionValue("configLocation"))
        val properties = new Properties()
        properties.load(SparkUtils.getClass.getResourceAsStream(cmd.getOptionValue("configLocation")))
        properties
      }
      else {
        val properties = new Properties()
        properties.load(SparkUtils.getClass.getResourceAsStream(PARMS))
        properties
      }
    }
    (properties, log)
  }

  def buildSparkSession(properties: Properties, appName : String = null, config : Map[String, String] = null): (SparkSession, String) = {
    val conf = new SparkConf(true).setAppName(if(appName == null) properties.getProperty("appName") else appName)

    conf.set("spark.scheduler.mode", properties.getProperty("spark.scheduler.mode"))
    conf.set("spark.executor.cores", properties.getProperty("cores"))
    conf.set("spark.executor.instances", properties.getProperty("instances"))
    conf.set("spark.task.maxFailures", properties.getProperty("spark.task.maxFailures"))
    conf.set("spark.blacklist.enabled", properties.getProperty("spark.blacklist.enabled"))
    conf.set("spark.blacklist.task.maxTaskAttemptsPerExecutor", properties.getProperty("spark.blacklist.task.maxTaskAttemptsPerExecutor"))
    conf.set("spark.blacklist.task.maxTaskAttemptsPerNode", properties.getProperty("spark.blacklist.task.maxTaskAttemptsPerNode"))
    conf.set("spark.blacklist.stage.maxFailedTasksPerExecutor", properties.getProperty("spark.blacklist.stage.maxFailedTasksPerExecutor"))
    conf.set("spark.blacklist.stage.maxFailedExecutorsPerNode", properties.getProperty("spark.blacklist.stage.maxFailedExecutorsPerNode"))
    conf.set("spark.dynamicAllocation.enabled", properties.getProperty("spark.dynamicAllocation.enabled"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.cassandra.connection.host", properties.getProperty("cassandra.host"))
    conf.set("spark.cassandra.auth.username", properties.getProperty("cassandra.username"))
    conf.set("spark.cassandra.auth.password", properties.getProperty("cassandra.password"))
    //    conf.set("hive.exec.dynamic.partition", "true")
    //    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    //    conf.set("spark.sql.shuffle.partitions", "10000")
    conf.set("spark.yarn.executor.memoryOverhead", properties.getProperty("spark.yarn.executor.memoryOverhead", "348"))


    if (config != null){
      config.foreach(ent => conf.set(ent._1,ent._2))
    }

    if (ide) {
      conf.setMaster("local[*]")
    }
    else {
      conf.setMaster("yarn")
    }
    val builder = SparkSession.builder().config(conf)
    builder.appName(appName)
    try {
      builder.enableHiveSupport()
    } catch {
      case e: IllegalArgumentException => println(s"Hive support not enabled - ${e.getMessage()}")
    }

    val sparkSession = builder.getOrCreate()
    sparkSession.setCassandraConf(ReadConf.SplitSizeInMBParam.option(properties.getProperty("cassandra.SplitSizeInMBParam")))


    val logMsg = "[" + appName + "] -- CustomMessage :"


    (sparkSession, logMsg)
  }

  private def getParser(args: Array[String]): CommandLine = {
    val parser = new BasicParser()
    val options = new Options()
      .addOption("configLocation", true, "enter config location")
      .addOption("log4j", true, "enter log4j location")
      .addOption("sql", true, "enter sql")

    parser.parse(options, args)
  }

  def getSql(args: Array[String]): String = {
    val cmd = getParser(args)
    return cmd.getOptionValue("sql")
  }

  private def getLog(path: String): Logger = {
    PropertyConfigurator.configure(getClass.getResource(path))
    @transient lazy val log = LogManager.getRootLogger
    log
  }
}
