package com.fx.demo.clickhouse

import java.util.Properties
import ru.yandex.clickhouse.ClickHouseDataSource
import ru.yandex.clickhouse.settings.ClickHouseProperties

object ClickhouseConnectionFactory extends Serializable{

  private val dataSources = scala.collection.mutable.Map[(String, Int), ClickHouseDataSource]()

  def get(host: String, port: Int = 8123): ClickHouseDataSource ={
//    dataSources.get((host, port)) match {
//      case Some(ds) =>
//        ds
//      case None =>
//        val ds = createDatasource(host, port = port)
//        dataSources += ((host, port) -> ds)
//        ds
//    }
    createDatasource(host, port = port)
  }

  private def createDatasource(host: String, dbO: Option[String] = None, port: Int = 8123) = {
    val props = new Properties()
//    props.setProperty("connection_timeout","2400000")
//    props.setProperty("keepAliveTimeout","3000000")
//    props.setProperty("socket_timeout","2400000")
    dbO map {db => props.setProperty("database", db)}

    val clickHouseProps = new ClickHouseProperties(props)
    new ClickHouseDataSource(s"jdbc:clickhouse://$host:$port", clickHouseProps)
  }
}