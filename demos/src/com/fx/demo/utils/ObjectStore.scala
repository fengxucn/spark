package com.fx.demo.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.javaswift.joss.client.factory.{AccountConfig, AccountFactory}
import org.javaswift.joss.model.{Account, Container}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object ObjectStore {
  val username  ="sw_svccatl"
  val tenantName ="sw_svccatl"
  val url_ndc = "https://host:port/v2.0/tokens"
  val url_cdc = "https://host:port/v2.0/tokens"
  val tenantId_ndc = "aa"
  val tenantId_cdc = "bb"
  val deleteAfter = 15 * 24 * 60 * 60
  val swiftPass_ndc="pwd"
  val swiftPass_cdc="pwd"
  val hostName = "SwiftHostName"

  private def createSwiftOSConnection(dc: String): Account = {

    val config = new AccountConfig
    config.setUsername(username)
    config.setTenantName(tenantName)
    if(dc == "cdc"){
      config.setAuthUrl(url_cdc)
      config.setPassword(swiftPass_cdc)
    }
    else{
      config.setAuthUrl(url_ndc)
      config.setPassword(swiftPass_ndc)
    }
    new AccountFactory(config).createAccount
  }

  def createSwiftHdfsConf(spark: SparkSession,dc: String = "ndc", hostName : String = hostName): Configuration ={

    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.set("fs.swift.impl", "org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem")
    hconf.set(s"fs.swift.service.$hostName.tenant", tenantName)
    hconf.set(s"fs.swift.service.$hostName.username", username)
    hconf.set(s"fs.swift.service.$hostName.http.port", "5000")
    hconf.set(s"fs.swift.service.$hostName.public", "false")
    hconf.set("fs.swift.connect.timeout","120000")
    hconf.set("fs.swift.socket.timeout","90000")
    hconf.set("fs.swift.connect.retry.count","10")
    hconf.set("fs.swift.connect.throttle.delay","3")
    if(dc == "cdc"){
      hconf.set(s"fs.swift.service.$hostName.auth.url", url_cdc)
      hconf.set(s"fs.swift.service.$hostName.password", swiftPass_cdc)
      hconf.set(s"fs.swift.service.$hostName.region", tenantId_cdc)
    }
    else{
      hconf.set(s"fs.swift.service.$hostName.auth.url", url_ndc)
      hconf.set(s"fs.swift.service.$hostName.password", swiftPass_ndc)
      hconf.set(s"fs.swift.service.$hostName.region", tenantId_ndc)
    }
    hconf
  }

  def getOrCreateContainer(spark : SparkSession, containerName: String, center : String = "ndc", hostName : String = hostName, public : Boolean = false): Container = {
    org.apache.hadoop.fs.FileSystem.get(createSwiftHdfsConf(spark, center, hostName))
    val account: Account = createSwiftOSConnection(center)
    val container: Container = account.getContainer(containerName)
    if(!container.exists()) {
      container.create()
      if (public)
        container.makePublic()
    }
    container
  }


  def rename(container: Container, srcName : String, dstName : String): Unit = {
    container.getObject(srcName).copyObject(container, container.getObject(dstName))
    container.getObject(srcName).delete()
  }

  //ttl seconds
  def ttl(container: Container, name : String, ttl : Long): Unit = {
    container.getObject(name).setDeleteAfter(ttl)
  }

  def getLinks(container: Container, path: String): List[String] = {
    if (!container.isPublic)
      container.makePublic()

    var i = 1
    val list = container.list().filter(o => o.getName.startsWith(s"$path/part"))

    list.foreach(o => {
      val ds = o.getName.split("\\.")
      val newName = (if (ds.length == 3) s"$path/part_${i}_of_${list.size}.${ds(1)}.${ds(2)}" else s"$path/part_${i}_of_${list.size}.${ds(1)}")
      rename(container, o.getName, newName)
      i += 1
    })

    val ans = ListBuffer.empty[String]
    container.list().filter(o => o.getName.startsWith(s"$path/part")).foreach(o => {
      ans += o.getPublicURL
    })

    container.list().filter(o => o.getName.startsWith(s"$path")).foreach(o => {
      o.setDeleteAfter(deleteAfter)
    })

    ans.toList
  }
}
