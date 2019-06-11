package com.fx.demo.hive

import com.fx.demo.utils.SparkUtils._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{explode, get_json_object, lit, udf}

import scala.collection.mutable.ListBuffer

object hive {
  def main(args: Array[String]): Unit = {
    val (properties, log) = loadProperties(args)
    val (spark, logMsg) = buildSparkSession(properties,"hive")

    val list = udf((gtin: String, gtins: String) => {
      val res = ListBuffer.empty[String]
      res.append(gtin)
      if (gtins != null) {
        res.append(gtins
          .substring(1, gtins.length - 1)
          .split(",")
          .map(r => {
            val str = r.split(":")(1)
            str.substring(1, str.length - 2)
          })
          .filter(e => e != null): _*)
      }
      res.toSet.toList
    })

    import spark.implicits._
    spark
      .sql("select id,name,json from table limit 100")
      .withColumn("empty_id", lit(""))
      .select($"id",$"name", get_json_object($"json","$.root.left[0].right").alias("right"), get_json_object($"json","$[0].info.type").alias("type"), $"empty_id")
      .distinct()
      .withColumn("id", list($"id", $"ids")) //ids is {"a:1","b:2","c:3"}
      .drop($"ids")
      .withColumn("id", explode($"id"))
      .distinct()
      .write
      .format(properties.getProperty("fileFormat"))
      .option("delimiter", ",")
      .option("header", "true")
      .option("escape", "\"")
      .option("quote", "\"")
      .option("codec", properties.getProperty("sparkCodeC"))
      .mode(SaveMode.Overwrite)
      .save("dir")

  }

}
