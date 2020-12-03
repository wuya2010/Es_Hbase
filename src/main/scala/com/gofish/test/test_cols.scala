package com.gofish.test

import common.conn_funcs
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

case class test(id:Int, date:String)
object test_cols {
  def main(args: Array[String]): Unit = {

    val conf =  new SparkConf().setMaster("local[*]").setAppName("xx")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //883584000000  694195200000  -28800000  1262275200000
    val  t = date_format(to_date(lit("-2707200000")),"yyyy-MM-dd")


    import spark.implicits._
    val rdd = sc.parallelize(Array(
//      test(1,"-2707200000"),
//      test(2,"1262275200000"),
//      test(3,"-63187200000"),
//      test(4,"883584000000"),
//      test(5,"694195200000"),
      test(6,"2020-02-24 21:21:21")
    ))

    val log_col =col("date").cast("long")
    val df_1 = rdd.toDF()
        .withColumn("new_date",when(log_col.isNotNull,log_col).otherwise(lit(0)))

// 时间戳转换为 时间, 2种格式
    val date_col = to_date(from_unixtime((col("date").cast("long"))/1000,"yyyy-MM-dd"))  //还是streing
    val df_2 =  rdd.toDF()
      .withColumn("new_date",when(date_col.isNotNull,date_col)
                  .otherwise(to_date(date_format(col("date"),"yyyy-MM-dd"))))  //统一时间格式

    val df_3 = rdd.toDF()
        .withColumn("new_date",date_format(col("date"),"yyyy-MM-dd HH:mm:ss"))


    //cast("long")
//    20/11/30 18:59:22 INFO DAGScheduler: Job 1 finished: show at test_cols.scala:37, took 0.036741 s
//      +---+-------------------+-------------+
//      | id|               date|     new_date|
//      +---+-------------------+-------------+
//      |  1|        -2707200000|  -2707200000|
//      |  2|      1262275200000|1262275200000|
//      |  3|2020-02-24 21:21:21|         null|
//      +---+-------------------+-------------+


//    df_1.schema.foreach(println)
//    println("没有转换")
    df_3.schema.foreach(println)

//    df_1.show() //new_date会有无法写入情况
//    df_2.show()

    df_3.show()

    /*
      1. 报错：st.EsHadoopRemoteException: mapper_parsing_exception: failed to parse field [new_date] of type [date] in document with id 'm6jKGHYBxSW-MaFcgCj9'. Preview of field's value: '-2707200000';org.elasticsearch.hadoop.rest.EsHadoopRemoteException: date_time_exception: date_time_exception: Invalid value for Year (valid values -999999999 - 999999999): -2707200000
	{"index":{}}
       说明： mapping 设置字段为 date ,不定义format
	    源字段为 string ,不处理会直接报错   Invalid value for Year (valid values -999999999 - 999999999): -2707200000

      2. 转换为
      (1)时间戳转换为 yyyy-MM-dd ，最终结果为 null, 写入无该字段
      (2)时间戳转换为 long ，无法写入date中，直接报错转换类型错误，Invalid value for Year (valid values -999999999 - 999999999): -2707200000

      3. 不设置mapping 字段时间类型，默认date都会转换为 时间错

      4. 设置format 后，df 强转类型也会报错，默认date类型就是时间戳

      5. df是long类型，相应的mapping也应该是long类型

     */


    //写入测试es中
//    df_2.write.format("org.elasticsearch.spark.sql").options(conn_funcs.test_options)
//      .mode(SaveMode.Overwrite).save("wang_test_date")




  }
}
