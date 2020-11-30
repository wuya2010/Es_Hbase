package com.gofish.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{date_format, lit}

case class test(id:Int, name:String)
object test_cols {
  def main(args: Array[String]): Unit = {

    val conf =  new SparkConf().setMaster("local[*]").setAppName("xx")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val  t = date_format(lit("2006-01-01"),"yyyy-MM-dd HH:mm:ss")
    import spark.implicits._
    val rdd = sc.parallelize(Array(test(1,"jim"))).toDF()
        .withColumn("date",t)

    rdd.show(false)

  }
}
