package com.gofish.trans

import common.CreateWinutils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object dm_company_person_es_flag {
  def main(args: Array[String]): Unit = {
    CreateWinutils.createWinutils()

    val conf :SparkConf = new SparkConf().setAppName("gofish_company_flag")//.setMaster("local[*]")
    val spark =SparkSession.builder().config(conf).getOrCreate()

    val test_options = Map(
      "es.net.http.auth.user"->"elastic",
      "es.net.http.auth.pass"->"H84I4fw6fDgdenuNRgfe",
      "es.nodes.wan.only" -> "true",
      "es.index.auto.create" -> "true",
      "es.nodes" -> "192.168.18.151:19200,192.168.18.149:19200"
    )

    val options = Map(
      "es.net.http.auth.user"->"elastic",
      "es.net.http.auth.pass"->"mxtZPtg0VYz2dxa907Oj",
      "es.nodes.wan.only" -> "true",
      "es.index.auto.create" -> "true",
      "es.nodes" ->"10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200"
    )


    //获取es数据 封装公共方法
    val  df_gofish_company = spark.read.format("es")
                            .options(options)
                            .load("dm_gofish_company")
                            //测试过滤条件
                            .filter(col("id").contains("221824f1b19b9d5f89299f62007bbdf5"))
                            .as("df_gofish_company")

    val  df_gofish_person = spark.read.format("es")
                            .options(options)
                            .load("dm_gofish_person")
                            //测试过滤条件
                            .filter(col("dm_gofish_company_id").contains("221824f1b19b9d5f89299f62007bbdf5"))
                            .select("dm_gofish_company_id")
                            .distinct()//有重复数据需要去重
                            .as("df_gofish_person")

    //       "company_flag" : {
    //          "type" : "long"
    //        },

    val df_gofish_company_flag = df_gofish_company.join(broadcast(df_gofish_person),expr("df_gofish_company.id=df_gofish_person.dm_gofish_company_id"),"left")
        .withColumn("company_flag",when(col("dm_gofish_company_id").isNull,lit(0)).otherwise(lit(1)))

    df_gofish_company.show()

    //写入ES
//    df_gofish_company_flag.write.format("org.elasticsearch.spark.sql").options(options)
//        .mode(SaveMode.Overwrite).save("df_gofish_company_flag")

    println("加载完成....")

  }
}
