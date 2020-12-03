package com.gofish.test

import common.conn_funcs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

object long_es {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("xx")//.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val rdd = sc.parallelize(Array(
      test(1, "-2707200000"),
      test(2, "1262275200000"),
      test(3, "-63187200000"),
      test(4, "883584000000"),
      test(5, "694195200000"),
      test(6, "2020-02-24 21:21:21")
    ))

    val log_col = col("date").cast("long")

    val df_1 = rdd.toDF()
      .withColumn("new_date", when(log_col.isNotNull, log_col).otherwise(lit(0)))



    //    df_1.schema.foreach(println)
    //    println("没有转换")
    df_1.schema.foreach(println)

    df_1.show()

    //写入测试es中
    df_1.write.format("org.elasticsearch.spark.sql").options(conn_funcs.options)
      .mode(SaveMode.Overwrite).save("wang_test_date")
  }
}
