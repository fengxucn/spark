package com.fx.demo.utils

import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.util.{Base64, Calendar, Properties, UUID}

import com.fx.demo.utils.Email.EmailProp
import com.fx.demo.utils.SparkUtils.loadProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.http.StatusLine
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.udf
import org.javaswift.joss.model.Container
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Utils {

  def snakecaseify(s: String): String = {
    s.toLowerCase().replace(" ", "_")
  }

  //converts all the column names of a DataFrame to snake_case
  def snakeCaseColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (acc, cn) =>
      acc.withColumnRenamed(cn, snakecaseify(cn))
    }
  }

  //read data from hdfs to memory
  def readResult(path: String, sc: SparkContext): (Array[Byte], String) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val file = fs.globStatus(new Path(path + "/part-*"))(0).getPath()
    val fileName = file.getName
    val stream = fs.open(file)
    val ans = new Array[Byte](stream.available())
    stream.readFully(ans)
    (ans, fileName.substring(fileName.lastIndexOf(".")))
  }


  //change hdfs file name
  def changeFileName(path: String, sc: SparkContext, name: String): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val uuid = UUID.randomUUID()
    fs.globStatus(new Path(path + "/part*")).foreach(file => {
      val oldName = file.getPath.getName
      val datas = oldName.split("-")
      val newName = name + "-" + datas(1) + "-" + uuid + "-" + datas(datas.length - 1)
      fs.rename(new Path(path + "/" + oldName), new Path(path + "/" + newName))
    })
  }

  private def email(properties: Properties, emails: String, subject: String, content: String): Unit = {
    val emailProp = Email.getEmailProperties(properties)
    emailProp.toEmail = emails
    Email.triggerEmail(emailProp, subject, content)
  }

  private def merge(hdfs: String, properties: Properties, sparkContext: SparkContext, prefixName: String): Unit = {
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    FileUtil.copyMerge(fs, new Path(hdfs), fs, new Path(s"$hdfs$prefixName.csv.gz"), false, sparkContext.hadoopConfiguration, null)
  }

  //load file from hdfs and save to sharepoint
  def save(hdfs: String, properties: Properties, sparkContext: SparkContext, prefixName: String): (Int, String) = {
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val urlList = ListBuffer.empty[String]
    var i = 1

    fs.globStatus(new Path(hdfs + "/part-*")).foreach(file => {
      val fileName = file.getPath.getName
      val stream = fs.open(file.getPath)
      val ans = new Array[Byte](stream.available())
      stream.readFully(ans)

      val fileType = fileName.substring(fileName.lastIndexOf("."))

      val (result, url) = writeResult(ans, prefixName + i + ".csv" + fileType, properties)
      i = i + 1

      if (result.getStatusCode == 200) {
        urlList += url
      } else {
        return (result.getStatusCode, result.getReasonPhrase)
      }
    })

    rmDir(hdfs, sparkContext)


    return (200, urlList.mkString("\n"))
  }

  val folderName = "Shared%20Documents/demo"
  def writeResult(data: Array[Byte], fileName : String, properties : Properties) : (StatusLine, String) = {
    val host = properties.getProperty("sharePoint")
    val refreshToken = properties.getProperty("refresh_token")

    val url = host + "_api/Web/GetFolderByServerRelativeUrl('"+ folderName +"')/Files/add(url='"+fileName+"',overwrite=true)"
    val post = new HttpPost(url)
    post.addHeader("Content-Type", "application/json;odata=verbose;charset=utf-8")
    post.setHeader("Authorization", Token.Token(refreshToken).getToken())

    post.setConfig(RequestConfig.custom.setConnectTimeout(1200000).setConnectionRequestTimeout(1200000).build)

    post.setEntity(new ByteArrayEntity(data))

    val resp = HttpClients.createDefault().execute(post)
    try {
      return (resp.getStatusLine, host + folderName + "/" + fileName)
    } finally {
      resp.close()
    }


  }

  //zip dataframe and save to Object Store
  def zipAndLoad(spark: SparkSession, res: DataFrame, hdfs : String, container : Container, path : String): Unit ={
    res
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("codec", "uncompressed")
      .option("escape", "\"")
      .option("quote", "\"")
      .mode(SaveMode.Overwrite)
      .save(hdfs)

    container.getObject(s"$path/part-${UUID.randomUUID()}.csv.zip").uploadObject(zip(hdfs, spark.sparkContext, s"${path.replace("/","_")}.csv"))

    rmDir(hdfs, spark.sparkContext)
  }

  private def zip(hdfs: String, sparkContext: SparkContext, fileName : String) : Array[Byte] = {
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val stream = fs.open(fs.globStatus(new Path(hdfs + "/part-*"))(0).getPath)
    val ans = new Array[Byte](stream.available())
    stream.readFully(ans)
    zipBytes(fileName, ans)
  }


  private def zipBytes(filename: String, input: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(baos)
    val entry = new ZipEntry(filename)
    entry.setSize(input.length)
    zos.putNextEntry(entry)
    zos.write(input)
    zos.closeEntry()
    zos.close()
    baos.toByteArray
  }

  def rmFiles(list: List[String], sc : SparkContext): Unit ={
    val fs = FileSystem.get(sc.hadoopConfiguration)
    list.foreach(dir => {
      val path = new Path(dir);
      if (fs.exists(path))
        fs.delete(path, true)
    })

  }

  def rmDir(dir: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(dir);

    // delete existing directory

    if (fs.exists(path))
      fs.delete(path, true)
  }

  def yesterday: String = {
    getDay(-1)
  }

  def getDay(dif : Int) : String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, dif)
    format.format(cal.getTime)
  }

  private val MAX_SIZE = 500*1024*1024
  def getFileDirs(hdfs : String, sc: SparkContext, maxSizeBytes : Long = MAX_SIZE) : scala.List[String] = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val urlList = ListBuffer.empty[String]
    fs.globStatus(new Path(hdfs + "part-*.snappy.parquet")).foreach(file => {
      val fileName = file.getPath.getName
      if (fs.getFileStatus(file.getPath).getLen < maxSizeBytes)
        urlList += s"$hdfs$fileName"
    })

    urlList.toList
  }

  def isBigger(hdfs : String, sc: SparkContext, maxSizeBytes : Long) : Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    var sum: Long = 0
    fs.globStatus(new Path(hdfs + "part-*")).foreach(file => {
      sum += fs.getFileStatus(file.getPath).getLen
      if (sum >= maxSizeBytes)
        return true
    })
    false
  }

  def exist(dir: String, hadoopConfiguration: Configuration): Boolean = {
    try {
      FileSystem.get(hadoopConfiguration).globStatus((new Path(dir))).length > 0
    } catch {
      case exception: Exception => false
    }
  }


  def getTableNamesFromSql(sql: String, spark: SparkSession): Seq[String] = {
    val logical: LogicalPlan = spark.sessionState.sqlParser.parsePlan(sql)
    val tables = scala.collection.mutable.LinkedHashSet.empty[String]
    var i = 0
    while (true) {
      if (logical(i) == null) {
        return tables.toSeq
      } else if (logical(i).isInstanceOf[UnresolvedRelation]) {
        val tableIdentifier = logical(i).asInstanceOf[UnresolvedRelation].tableIdentifier
        tables += tableIdentifier.unquotedString.toLowerCase
      }
      i = i + 1
    }

    tables.toSeq
  }



  def md5(str: String): String = {
    MessageDigest.getInstance("MD5").digest(str.getBytes).map("%02x".format(_)).mkString
  }

  val addUTC = udf((time: String) => {
    if (time.endsWith("UTC]"))
      time
    else
      s"${time}[UTC]"
  })

}
