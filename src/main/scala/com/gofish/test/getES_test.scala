package com.gofish.test

import common.CreateWinutils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object getES_test {

  case class Test(name:String, id:Int)

  def main(args: Array[String]): Unit = {

    CreateWinutils.createWinutils()

    val conf :SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    conf.set("es.nodes","192.168.18.151:19200,192.168.18.149:19200")//设置在spark中连接es的url和端口
    conf.set("es.index.auto.create","true") //在spark中自动创建es中的索引
    conf.set("es.nodes.wan.only","true")

    val sc = new SparkContext(conf)
    //rdd写入es
    val test_rdd = sc.makeRDD(Array(Test("tom",1),Test("jim",2)))

//    println(test_rdd.collect().mkString(";"))

    //导入依赖

    //方式1
//    test_rdd.saveToEs("/test")

    //写入es
//    EsSpark.saveToEs(test_rdd,"/test")

//    val es_df1 = EsSpark.esRDD(sc, "/dm_gofish_media")
//
//    es_df1.foreach(println)

    val spark =SparkSession.builder().config(conf).getOrCreate()

    //读取es,设置es账号密码
    val options = Map(
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "192.168.18.151:19200,192.168.18.149:19200",
      "es.net.http.auth.user"->"elastic",
      "es.net.http.auth.pass"->"H84I4fw6fDgdenuNRgfe"
    )

    val df = spark.read.format("es")
        .options(options)
        .load("dm_gofish_media")


    //    df_gofish_company.createOrReplaceTempView("df_gofish_company")
    //    df_gofish_person.createOrReplaceTempView("df_gofish_person")

    df.show()

    println("over....")



  }

}
