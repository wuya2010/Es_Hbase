package com.gofish.Hbase

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/22 13:57
 */
object writeHbaseBulk2 {

  val hbaseQuorum = "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181"
  val hbaseClientPort = "2181"

  // 获取hbase配置
  def getHbaseConf(): Configuration = {
    var hbaseConf: Configuration = null
    try {
      hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", hbaseQuorum)
      hbaseConf.set("hbase.zookeeper.property.clientPort", hbaseClientPort)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    hbaseConf
  }

  // 获取Table对象, 通过传入的 table , 找到对应的名字
  def getTable(tableName: String): Table = {
    var table: Table = null
    try {
      val hbaseConf = getHbaseConf()
      val conn = ConnectionFactory.createConnection(hbaseConf)
      table = conn.getTable(TableName.valueOf(tableName))
    } catch {
      case e: Exception => e.printStackTrace()
    }
    table
  }

  // 大数据批量插入或更新hbase
  def insertBigData(tableName:String,rdd:RDD[(ImmutableBytesWritable,KeyValue)])={

    val conf = getHbaseConf()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //需要写入的表
    conf.set("hbase.mapreduce.hfileoutputformat.table.name","test_wang2") //输出表的位置

    val jobConf = new JobConf(conf)
    val load = new LoadIncrementalHFiles(conf)

    val hdfsFile="hdfs://192.168.18.148:8020/hfile"
    val path = new Path(hdfsFile)
    //如果存在则删除
    val fileSystem = FileSystem.get(URI.create(hdfsFile), new Configuration())
    if(fileSystem.exists(path)){
      fileSystem.delete(new Path(hdfsFile),true)
    }

    rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    //通过连接获取相关参数
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val locator = connection.getRegionLocator(TableName.valueOf(tableName))
    val table = connection.getTable(TableName.valueOf(tableName))
    //Thread.sleep(20000)
    //最终结果
    load.doBulkLoad(new Path(hdfsFile),admin,table,locator)
  }



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HbaseDemo")
      .master("local[*]")
      .getOrCreate()

    val rdd = spark.sparkContext.makeRDD(Array(
      "1,曹操,101","2,张辽,102","3,夏侯惇,103"
    )).map(t=>{
      val arr = t.split(",")
      (arr(0),arr(1),arr(2))
    })


    val rdd2: RDD[(ImmutableBytesWritable, KeyValue)] = spark.sparkContext.makeRDD(Array(
      "4,刘备,104","5,关羽,105","6,张飞,106"))
      .map(t=>{
      var kvList : Seq[KeyValue] = List()
      val arr = t.split(",")
      val kvName=new KeyValue(arr(0).getBytes,"info".getBytes,"name".getBytes,arr(1).getBytes)
      val kvAge=new KeyValue(arr(0).getBytes,"info".getBytes,"age".getBytes,arr(2).getBytes)
        //解析rdd
      kvList = kvList :+ kvAge
      kvList = kvList :+ kvName
      (new ImmutableBytesWritable(arr(0).getBytes),kvList)
    }).flatMapValues(_.iterator)

    //解析rdd后插入数据
    insertBigData("test_wang2",rdd2)


    println("=============Succeed=================")

  }

}
