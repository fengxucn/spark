package com.fx.demo.clickhouse

import ru.yandex.clickhouse.ClickHouseDataSource
import com.fx.demo.clickhouse.Utils._


case class ClickhouseClient(clusterNameO: Option[String] = None)
                           (implicit ds: ClickHouseDataSource){
  import com.fx.demo.clickhouse.ClickhouseResultSetExt._
  def createDb(dbName: String){
    query(s"create database if not exists $dbName")
  }

  def dropDb(dbName: String){
    query(s"DROP DATABASE IF EXISTS $dbName")
  }

  def query(sql: String) = {
    using(ds.getConnection){ conn =>
      val statement = conn.createStatement()
      val rs = statement.executeQuery(sql)
      rs
    }
  }

  def queryCluster(sql: String) = {
    val resultSet = runOnAllNodes(sql)
    ClusterResultSet(resultSet)
  }

  def createDbCluster(dbName: String) = {
    runOnAllNodes(s"create database if not exists $dbName")
      .count(x => x._2 == null)
  }

  def dropDbCluster(dbName: String) = {
    runOnAllNodes(s"DROP DATABASE IF EXISTS $dbName")
      .count(x => x._2 == null)
  }

  def getClusterNodes() = {
    val clusterName = isClusterNameProvided()
    using(ds.getConnection) { conn =>
      val statement = conn.createStatement()
      val rs = statement.executeQuery(s"select host_name, host_address from system.clusters where cluster == '$clusterName'")
      val r = rs.map(x => x.getString("host_name"))
      require(r.nonEmpty, s"cluster $clusterNameO not found")
      r
    }
  }

  private def runOnAllNodes(sql: String) = {
    getClusterNodes().map{ nodeIp =>
      val nodeDs = ClickhouseConnectionFactory.get(nodeIp)
      val client = ClickhouseClient()(nodeDs)
      (nodeIp, client.query(sql))
    }
  }

  private def isClusterNameProvided() = {
    clusterNameO match {
      case None => throw new Exception("cluster name is requires")
      case Some(clusterName) => clusterName
    }
  }
}