package com.gofish.test

import common.conn_funcs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, from_unixtime, lit, to_date, when}

/**
 *
 * 写入结果是 时间戳， 并且字段统一 不能完成
 */
object date_string_es {
  def main(args: Array[String]): Unit = {

    val conf =  new SparkConf().setAppName("xx")//.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //883584000000  694195200000  -28800000  1262275200000
    val  t = date_format(to_date(lit("-2707200000")),"yyyy-MM-dd")


    import spark.implicits._
    val rdd = sc.parallelize(Array(
      test(1,"-2707200000"),
      test(2,"1262275200000"),
      test(3,"-63187200000"),
      test(4,"883584000000"),
      test(5,"694195200000"),
      test(6,"2020-02-24 21:21:21")
    ))


    // 时间戳转换为 时间, 2种格式
    val date_col = to_date(from_unixtime((col("date").cast("long"))/1000,"yyyy-MM-dd"))  //还是streing
    val df_2 =  rdd.toDF()
      .withColumn("new_date",when(date_col.isNotNull,date_col)
        .otherwise(to_date(date_format(col("date"),"yyyy-MM-dd"))))  //统一时间格式


    df_2.schema.foreach(println)

    df_2.show()



    //    //写入测试es中
        df_2.write.format("org.elasticsearch.spark.sql").options(conn_funcs.options)
          .mode(SaveMode.Overwrite).save("wang_test_date")




  }
}
