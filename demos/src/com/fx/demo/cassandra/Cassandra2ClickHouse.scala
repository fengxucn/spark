package com.fx.demo.cassandra

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import com.fx.demo.clickhouse.ClickhouseSparkExt._


object Cassandra2ClickHouse {
  private def test2(spark : SparkSession): Unit ={
    /*
    table schema:
    CREATE TABLE IF NOT EXISTS castar.audit_events \
( \
    date DateTime, \
    type String, \
    id String, \
    hash String, \
    tags Nested( \
        name String, \
        value Nullable(String) \
    ), \
    changes Nested( \
        name String, \
        current Nullable(String), \
        previous Nullable(String) \
    ) \
) \
ENGINE = MergeTree \
PARTITION BY date \
ORDER BY (type, id)
     */
    import spark.implicits._

    val datas = List(
      ("2019-03-14 14:27:00", "TEST", "373130b5-f3e0-42cf-9528-de4d762a0ed7","4727396d3e551fdac3fb3b362f74ed8c", List("tags_1", "tags_2"), List("tags_v_1","tags_v_2"), List("atts_name_1","atts_name_2"), List("att_v_1","att_v_2"), List("att_v_1","att_v_2")),
      ("2019-03-14 14:27:00", "TEST", "373130b5-f3e0-42cf-9528-de4d762a0ed7","4727396d3e551fdac3fb3b362f74ed8c", List("tags_1", "tags_2"), List("tags_v_1",null), List("atts_name_1","atts_name_2"), List("att_v_1",null), List(null, null)))
      .toDF("date", "type", "id", "hash", "t.name", "t.value", "c.name","c.current","c.previous")

    datas.save("127.0.0.1",8123 ,"db","tablename")
    println("Success")

  }

  private def test(spark : SparkSession): Unit ={
    /*
    Table schema:
    CREATE TABLE IF NOT EXISTS castar.audit_events \
( \
    date DateTime, \
    type String, \
    id String, \
    tags Nested( \
        name String, \
        value Nullable(String) \
    ) \
) \
ENGINE = MergeTree \
PARTITION BY date \
ORDER BY (type, id)
     */
    import spark.implicits._
    // Saving data to a JDBC source
    val datas = List(("2019-03-14 14:27:00", "TEST", "testID", List("tags_1", "tags_2"), List(null,null)),
      ("2019-03-15 14:27:00", "TEST", "testID1", List("tags_1", "tags_2","tags_3"), List("tags_v_1","tags_v_2", null)),
      ("2019-03-15 11:27:00", "TEST", "testID2", List("tags_1"), List("tags_v_1")),
      ("2019-03-15 12:27:00", "TEST", "testID1", List("tags_1", "tags_2"), List("tags_v_1",null)))
      .toDF("date", "type", "id", "tags.name", "tags.value")

    datas.save("127.0.0.1",8123 ,"db","tablename")

  }

  def write(data: DataFrame, jdbcUrl: String, tableName: String): Unit = {
    JdbcDialects.registerDialect(ClickHouseDialect)

    val props = new java.util.Properties()
    props.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    props.put("connection", jdbcUrl)

    val repartionedData = data.repartition(100)

    repartionedData.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, tableName, props)

    JdbcDialects.unregisterDialect(ClickHouseDialect)

  }

  private object ClickHouseDialect extends JdbcDialect {
    //override here quoting logic as you wish
    override def quoteIdentifier(colName: String): String = colName

    override def canHandle(url: String): Boolean = url.startsWith("jdbc:clickhouse")
  }
}
