package com.gofish.test

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object get_hbase {
  def main(args: Array[String]): Unit = {
    //build spark
    val conf = new SparkConf().setAppName("get_hbase").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val DM_GOFISH_COMPANY = "dm_gofish_company"
    val DM_GOFISH_PERSON= "dm_gofish_person"

    //建立连接
    val HbaseConf = HBaseConfiguration.create()

    HbaseConf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
    HbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    HbaseConf.set(TableInputFormat.INPUT_TABLE, "test:01")


    //读取hbase数据转换为rdd
    val result_rdd = sc.newAPIHadoopRDD(
      HbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

//    result_rdd.map {
//      case (key, value) => {
//
//        val cells = value.listCells()
//        import scala.collection.JavaConversions._
//        for (cell <- cells) {
//          println("result...")
//          println(Bytes.toString(CellUtil.cloneQualifier(cell)))
//        }
//      }
//    }


//    DwdQzPaperView(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime,
//      conteststarttime, contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus,
//      description, papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)

    import spark.implicits._

    //将hbase数据转换为df
    val test_rdd = result_rdd.map(x=>(
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("age")))
      ,
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))
    )).toDF("age","name")


    test_rdd.show(false)


//    println(result_rdd.collect().mkString(";"))


    }

  }
