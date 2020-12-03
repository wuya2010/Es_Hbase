package com.gofish.trans


import com.gofish.bean.Gofishompany
import common.conn_funcs
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object dm_company_person_hbase_flag {

  def main(args: Array[String]): Unit = {
    //build spark
    val conf = new SparkConf().setAppName("get_hbase")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val DM_GOFISH_COMPANY = "dm:gofish_company"
    val DM_GOFISH_PERSON= "dm:gofish_person"

    import spark.implicits._

    //将hbase数据转换为df
    val company_rdd = conn_funcs.get_hbase_rdd(sc,DM_GOFISH_COMPANY)
    val person_rdd = conn_funcs.get_hbase_rdd(sc,DM_GOFISH_PERSON)


    //根据样例类字段生成df
    val company_df = company_rdd.map(x=>(
      Gofishompany(
      Bytes.toString(x._2.getRow),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("id"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("address"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("address_new"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("apply_date"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("apply_location"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("apply_man"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("apply_num"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("apply_pub_num"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("apply_type"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_location"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_name"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_num"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_register_num"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brand_type"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("brief"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("business_model"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("business_type"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("c_url"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("c_url2"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("c_url_new"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("c_url_status_new"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("city"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_dynamic_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_dynamic_title"))),
//      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_dynamic_title"))),  //重复字段
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_mainMarket"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_num"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("company_state"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("country"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("country_code"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("country_code_iso"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("country_code_iso2"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("country_region"))),
//      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("create_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("custom_status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("district"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("dm_gofish_industry_id"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("domain_status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("email"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("facebook"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("fax"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("financing"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("financing_investor"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("financing_value"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("img_src_keyword"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("img_url_keyword"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("importExport"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("instagram"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("investments_abroad_amount"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("investments_abroad_investor"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("investments_abroad_time"))),  //加载报错
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("is_delete"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("kw"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("legal_proceedings_accused"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("legal_proceedings_accuser"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("legal_proceedings_event"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("legal_proceedings_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("legend_person"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("legend_person_title"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("linkedin"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("linkedin_status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("logo"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("logo_src_keyword"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("logo_url_keyword"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("main_products"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("main_status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("markers"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("marketing_annual_marketing_appraisement"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("marketing_annual_marketing_turnover"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("marketing_estimated_value"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("marketing_retained_profits"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("marketing_the_tax_credit_rating"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("media_status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("new_address"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("obj_name"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("patent_expiration_date"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("patent_name"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("person_status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("postcode_keyword"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("process_version"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("province"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("published_date"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("qualification_certificate_expire_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("qualification_certificate_img"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("qualification_certificate_name"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("qualification_certificate_start_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("recruitment_information_job_type"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("recruitment_information_number_of_people"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("recruitment_information_post_desc"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("recruitment_information_post_name"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("recruitment_information_update_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("recruitment_information_work_place"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("register_id"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("registered_capital"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("share_aggregate_market_value"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("share_eps"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("share_pe"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("sort_long"))), //转long
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("status"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("street"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tel"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("tel_new"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("the_enterprise_referred_to_as"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("total_employees"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("total_employees_new"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("total_revenue"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("twitter"))),
//      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("update_time"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("vocation"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("year_established"))),
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("year_established_new"))), //需要转换为date格式
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("youtube"))))
    ))
      .toDF().repartition(200).as("df_gofish_company")

    val person_df = person_rdd.map(x=>(
      Bytes.toString(x._2.getValue(Bytes.toBytes("info"),Bytes.toBytes("dm_gofish_company_id")))
      ))
      .toDF("dm_gofish_company_id").distinct().repartition(200).as("df_gofish_person")

    //时间字段统一
    val date_col = from_unixtime((col("year_established_new").cast("long"))/1000,"yyyy-MM-dd")
    val format_date_col = when(date_col.isNotNull,date_col).otherwise(date_format(col("year_established_new"),"yyyy-MM-dd"))

    //根据rowkey进行关联，如果没有该字段则为空
    val df_gofish_company = company_df.join(person_df,expr("df_gofish_company.row_key=df_gofish_person.dm_gofish_company_id"),"left")
      .withColumn("is_delete",coalesce(col("is_delete").cast("int"),lit(0)))
      .withColumn("sort_long",coalesce(col("sort_long").cast("long"),lit(0)))
      .withColumn("status",coalesce(col("status").cast("long"),lit(0)))//如果为空，转为0
      .withColumn("year_established_new",format_date_col)
      .withColumn("staff_status",when(col("dm_gofish_company_id").isNull,lit(0)).otherwise(lit(1)).cast("int"))
      .repartition(200)


    /**
     * 1. 根据表直接提到的 id , 主表无法关联
     * 2. 根据row_kew 进行关联很多结果集也为空
     */
    println("结果集数据条数")
    df_gofish_company.filter(col("staff_status")=!=lit(0)).show(100,false)

    val result = df_gofish_company
        .withColumn("id",col("row_key")) //用 rowkey 的值替换 id
        .drop("dm_gofish_company_id","row_key")

    // 写入ES
    result.write.format("org.elasticsearch.spark.sql").options(conn_funcs.options)
      .mode(SaveMode.Overwrite).save("dm_gofish_company_flag")

    println(s"${DM_GOFISH_COMPANY}加载完成....")

    }

  }






/**
 * //将空id 与 非空id 分别进行处理， 这里
 * val company_df_notnull = company_df.filter(col("id").isNotNull)
 * //时间字段统一
 * val date_col = from_unixtime((col("year_established_new").cast("long"))/1000,"yyyy-MM-dd")
 * val format_date_col = when(date_col.isNotNull,date_col).otherwise(date_format(col("year_established_new"),"yyyy-MM-dd"))
 *
 * val company_df_null = company_df.filter(col("id").isNull)
 * .withColumn("is_delete",col("is_delete").cast("int"))
 * .withColumn("sort_long",col("sort_long").cast("long"))
 * .withColumn("status",col("status").cast("long"))
 * //      .withColumn("year_established_new",to_date(col("year_established_new")))
 * .withColumn("year_established_new",format_date_col)
 * .withColumn("staff_status",lit(0).cast("int"))//空则一定没有关联上
 * .repartition(200)
 *
 * val df_gofish_company_1 = company_df_notnull.join(person_df,expr("df_gofish_company.id=df_gofish_person.dm_gofish_company_id"),"left")
 * .withColumn("is_delete",col("is_delete").cast("int"))
 * .withColumn("sort_long",col("sort_long").cast("long"))
 * .withColumn("status",col("status").cast("long"))
 * .withColumn("year_established_new",format_date_col)
 * .withColumn("staff_status",when(col("dm_gofish_company_id").isNull,lit(0)).otherwise(lit(1)).cast("int"))
 * .drop("dm_gofish_company_id")
 * .repartition(200)
 *
 * val df_gofish_company = df_gofish_company_1.unionByName(company_df_null)
 *
 * //    df_gofish_company.schema.foreach(println)
 * //    df_gofish_company.show(false)
 * println("非空id数据集")
 * company_df_notnull.show()
 * println(company_df.count)
 * println("空id数据集")
 * company_df_null.show()
 * println(company_df_null.count)
 *
 * println("非空join结果")
 * df_gofish_company_1.show()
 */

