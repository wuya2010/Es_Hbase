package com.gofish.test

import java.net.URI

import com.gofish.test.writeHbaseBulk.tableName
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/22 17:16
 */
object writeHbaseBulk3 {

  case class gofishCompany(
                            rowkey: String,
                            address: String,
                            address_new: String,
                            apply_date: String,
                            apply_location: String,
                            apply_man: String,
                            apply_num: String,
                            apply_pub_num: String,
                            apply_type: String,
                            brand: String,
                            brand_location: String,
                            brand_name: String,
                            brand_num: String,
                            brand_register_num: String,
                            brand_time: String,
                            brand_type: String,
                            brief: String,
                            business_model: String,
                            business_type: String,
                            c_url: String,
                            c_url2: String,
                            c_url_new: String,
                            c_url_status_new: String,
                            city: String,
                            company_dynamic_time: String,
                            company_dynamic_title: String,
                            company_mainMarket: String,
                            company_num: String,
                            company_state: String,
                            country: String,
                            country_code: String,
                            country_code_iso: String,
                            country_code_iso2: String,
                            country_region: String,
                            custom_status: String,
                            district: String,
                            dm_gofish_industry_id: String,
                            domain_status: String,
                            email: String,
                            facebook: String,
                            fax: String,
                            financing: String,
                            financing_investor: String,
                            financing_value: String,
                            img_src_keyword: String,
                            img_url_keyword: String,
                            importExport: String,
                            instagram: String,
                            investments_abroad_amount: String,
                            investments_abroad_investor: String,
                            investments_abroad_time: String,
                            is_delete: String, //Int
                            kw: String,
                            legal_proceedings_accused: String,
                            legal_proceedings_accuser: String,
                            legal_proceedings_event: String,
                            legal_proceedings_time: String,
                            legend_person: String,
                            legend_person_title: String,
                            linkedin: String,
                            linkedin_status: String,
                            logo: String,
                            logo_src_keyword: String,
                            logo_url_keyword: String,
                            main_products: String,
                            main_status: String,
                            markers: String,
                            marketing_annual_marketing_appraisement: String,
                            marketing_annual_marketing_turnover: String,
                            marketing_estimated_value: String,
                            marketing_retained_profits: String,
                            marketing_the_tax_credit_rating: String,
                            media_status: String,
                            new_address: String,
                            obj_name: String,
                            patent_expiration_date: String,
                            patent_name: String,
                            person_status: String,
                            postcode_keyword: String,
                            process_version: String,
                            province: String,
                            published_date: String,
                            qualification_certificate_expire_time: String,
                            qualification_certificate_img: String,
                            qualification_certificate_name: String,
                            qualification_certificate_start_time: String,
                            recruitment_information_job_type: String,
                            recruitment_information_number_of_people: String,
                            recruitment_information_post_desc: String,
                            recruitment_information_post_name: String,
                            recruitment_information_update_time: String,
                            recruitment_information_work_place: String,
                            register_id: String,
                            registered_capital: String,
                            share_aggregate_market_value: String,
                            share_eps: String,
                            share_pe: String,
                            sort_long: String, //long
                            status: String, //long
                            street: String,
                            tel: String,
                            tel_new: String,
                            the_enterprise_referred_to_as: String,
                            total_employees: String,
                            total_employees_new: String,
                            total_revenue: String,
                            twitter: String,
                            vocation: String,
                            year_established: String,
                            year_established_new: String, //转date类型
                            youtube: String,
                            order_industry:String,
                            product_status:String
                          )

  case class testHbase(rowkey:String,name:String,id:Int,money:Long)
  case class testHbase2(rowkey:String,address:String,order_industry:String,youtube:String)

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

//  // 获取Table对象, 通过传入的 table , 找到对应的名字
//  def getTable(tableName: String): Table = {
//    var table: Table = null
//    try {
//      val hbaseConf = getHbaseConf()
//      val conn = ConnectionFactory.createConnection(hbaseConf)
//      table = conn.getTable(TableName.valueOf(tableName))
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//    table
//  }

  // 大数据批量插入或更新hbase
  def insertBigData(tableName:String,rdd:RDD[(ImmutableBytesWritable,KeyValue)])={

    var conf: Configuration = null
    try {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", hbaseQuorum)
      conf.set("hbase.zookeeper.property.clientPort", hbaseClientPort)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //需要写入的表
    conf.setInt("hbase.rpc.timeout", 20000)
    conf.setInt("hbase.client.operation.timeout", 30000)
    conf.setInt("hbase.client.scanner.timeout.period", 200000)
    //输出表的位置
    conf.set("hbase.mapreduce.hfileoutputformat.table.name",tableName)

    //相关配置
    val hdfsFile="hdfs://192.168.18.148:8020/HbaseTemp/hfile"
    val path = new Path(hdfsFile)
    //如果存在则删除
    val fileSystem = FileSystem.get(URI.create(hdfsFile), new Configuration())
    if(fileSystem.exists(path)){
      fileSystem.delete(new Path(hdfsFile),true)
    }

    //通过连接获取相关参数
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val locator = connection.getRegionLocator(TableName.valueOf(tableName))
    val table = connection.getTable(TableName.valueOf(tableName))

    //表描述器
    val TableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    //如果表不存在，则创建表
    if(!admin.tableExists(TableName.valueOf(tableName))){
      TableDescriptor.addFamily(new HColumnDescriptor("info"))//默认新建表 family
      admin.createTable(TableDescriptor)
    }

    rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    //Thread.sleep(20000)
    //最终结果
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(hdfsFile),admin,table,locator)
  }


//原来的
  def writeBigDf2Hbase2(tableName:String,df:DataFrame, Family: String,rowkey:String,rdd:RDD[(ImmutableBytesWritable,KeyValue)] = null)={

    var conf: Configuration = null
    try {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
    } catch {
      case e: Exception => e.printStackTrace()
    }

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //需要写入的表
    conf.setInt("hbase.rpc.timeout", 20000)
    conf.setInt("hbase.client.operation.timeout", 30000)
    conf.setInt("hbase.client.scanner.timeout.period", 200000)
    //输出表的位置
    conf.set("hbase.mapreduce.hfileoutputformat.table.name",tableName)

    //相关配置
    val hdfsFile="hdfs://192.168.18.148:8020/HbaseTemp/hfile"
    val path = new Path(hdfsFile)
    //如果存在则删除
    val fileSystem = FileSystem.get(URI.create(hdfsFile), new Configuration())
    if(fileSystem.exists(path)){
      fileSystem.delete(new Path(hdfsFile),true)
    }

    //通过连接获取相关参数
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val locator = connection.getRegionLocator(TableName.valueOf(tableName))
    val table = connection.getTable(TableName.valueOf(tableName))

    //表描述器
    val TableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    //如果表不存在，则创建表
    if(!admin.tableExists(TableName.valueOf(tableName))){
      TableDescriptor.addFamily(new HColumnDescriptor(Family))//默认新建表 family
      admin.createTable(TableDescriptor)
    }


    /**
     * 解析rdd
     */

    //需要将schema 中的rowkey过滤掉
    val name_field = df.schema.fields.map(x=>(x.name,x.dataType)).toSeq.diff(Seq((rowkey,StringType))).sortBy(x=>x._1)

    val t =  df.rdd.sortBy(x=>x.getAs[String](rowkey))
    val trans_rdd = df.rdd.sortBy(x=>x.getAs[String](rowkey)).map(row=>{
      var kvList : Seq[KeyValue] = List()
      val getRowkey = row.getAs[String](rowkey)
      //遍历循环
      name_field.map(y=>{
        y._2 match {
          //hbase 写入是否考虑字段类型
          //new KeyValue(arr(0).getBytes,"info".getBytes,"age".getBytes,arr(2).getBytes)
          case StringType => {
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey),Bytes.toBytes(Family),Bytes.toBytes(y._1),Bytes.toBytes(row.getAs[String](y._1)))
            kvList
          }
          case IntegerType => {
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey),Bytes.toBytes(Family),Bytes.toBytes(y._1),Bytes.toBytes(row.getAs[Int](y._1)))
            kvList
          }
          case LongType => {
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey),Bytes.toBytes(Family),Bytes.toBytes(y._1),Bytes.toBytes(row.getAs[Long](y._1)))
            kvList
          }
          //case _ => throw new Exception("Pleace check your fields type....")
        }
      })
      (new ImmutableBytesWritable(Bytes.toBytes(getRowkey)),kvList)
    }).flatMapValues(_.iterator)


    trans_rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    //最终结果
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(hdfsFile),admin,table,locator)
  }




  def writeBigDf2Hbase(tableName:String,df:DataFrame, Family: String,rowkey:String,rdd:RDD[(ImmutableBytesWritable,KeyValue)] = null)={

    var conf: Configuration = null
    try {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
    } catch {
      case e: Exception => e.printStackTrace()
    }

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //需要写入的表
    conf.setInt("hbase.rpc.timeout", 20000)
    conf.setInt("hbase.client.operation.timeout", 30000)
    conf.setInt("hbase.client.scanner.timeout.period", 200000)
    //输出表的位置
    conf.set("hbase.mapreduce.hfileoutputformat.table.name",tableName)

    //相关配置
    val hdfsFile="hdfs://192.168.18.148:8020/HbaseTemp/hfile"
    val path = new Path(hdfsFile)
    //如果存在则删除
    val fileSystem = FileSystem.get(URI.create(hdfsFile), new Configuration())
    if(fileSystem.exists(path)){
      fileSystem.delete(new Path(hdfsFile),true)
    }

    //通过连接获取相关参数
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val locator = connection.getRegionLocator(TableName.valueOf(tableName))
    val table = connection.getTable(TableName.valueOf(tableName))

    //表描述器
    val TableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    //如果表不存在，则创建表
    if(!admin.tableExists(TableName.valueOf(tableName))){
      TableDescriptor.addFamily(new HColumnDescriptor(Family))//默认新建表 family
      admin.createTable(TableDescriptor)
    }


    /**
     * 解析rdd
     */

    //需要将schema 中的rowkey过滤掉
    //使用bulk批量写入，需要rowkey + column名 都有序，这里对column名进行排序
    val name_field = df.schema.fields.map(x=>(x.name,x.dataType)).toSeq.diff(Seq((rowkey,StringType))).sortBy(x=>x._1)

    val trans_rdd = df.rdd.sortBy(x=>x.getAs[String](rowkey)).map(row=>{
      //        var kvList : Seq[KeyValue] = mutable.List()
      var kvList: mutable.Seq[KeyValue] = mutable.ListBuffer()
      val getRowkey = row.getAs[String](rowkey)
      //遍历循环
      name_field.map(y=>{
        y._2 match {
          //hbase 写入是否考虑字段类型
          case StringType => {
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey),Bytes.toBytes(Family),Bytes.toBytes(y._1),Bytes.toBytes(row.getAs[String](y._1)))
            kvList
          }
          case IntegerType => {
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey),Bytes.toBytes(Family),Bytes.toBytes(y._1),Bytes.toBytes(row.getAs[Int](y._1)))
            kvList
          }
          case LongType => {
            kvList = kvList :+ new KeyValue(Bytes.toBytes(getRowkey),Bytes.toBytes(Family),Bytes.toBytes(y._1),Bytes.toBytes(row.getAs[Long](y._1)))
            kvList
          }
          case _ => throw new Exception("Pleace check your fields type....")
        }
      })
      (new ImmutableBytesWritable(Bytes.toBytes(getRowkey)),kvList)
    }).flatMapValues(_.iterator)


    trans_rdd.saveAsNewAPIHadoopFile(hdfsFile,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    //最终结果
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(hdfsFile),admin,table,locator)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HbaseDemo")
      .master("local[*]")
      .getOrCreate()

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
//    insertBigData("test_wang2",rdd2)

   // Added a key not lexically larger than previou
    import spark.implicits._
    val test_df = spark.sparkContext.parallelize(Array(
      gofishCompany("0b81f0bf3747cbaf27a5a895a47c1dd9", "Israel J Diaz P  ","我是大众故宫","","","","","","","","","","","","","","","","","https://calle londres edif ius ph1, las merc caracas miranda  ","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","",""),
      gofishCompany("0b81f0bf3747cbaf27a5a895a47c1dd9", "Israel J Diaz P  ","我是大众故宫","","","","","","","","","","","","","","","","","https://calle londres edif ius ph1, las merc caracas miranda  ","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","",""),
      gofishCompany("1d3e05c1a0aae71c5586b142c2324384","aaaaaaaaaaaaaaaaaaaaaaaa","我是大众故宫","","","","","","","","","","","","","","","","","https://calle londres edif ius ph1, las merc caracas miranda  ","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","")
    )).toDF()
    //解析rdd后插入数据
    writeBigDf2Hbase("test_wang3",test_df,"info","rowkey")


    println("=============Succeed=================")

  }



}
