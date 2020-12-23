package com.gofish.test

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/22 19:28
 */
object test3 {

  def main(args: Array[String]): Unit = {

    var kvList : Seq[KeyValue] = List()

    val test_set = Seq("1","2","3")
    //遍历循环
    test_set.map(x=>{
    x match {
        //hbase 写入是否考虑字段类型
        case "1" => {
          kvList = kvList :+ new KeyValue(Bytes.toBytes("130"),Bytes.toBytes("f"),Bytes.toBytes("name"),Bytes.toBytes(" aabc"))
          kvList
        }
        case "2"=> {
          kvList = kvList :+ new KeyValue(Bytes.toBytes("130"),Bytes.toBytes("f"),Bytes.toBytes("id"),Bytes.toBytes("1"))
          kvList
        }
        case "3" => {
          kvList = kvList :+ new KeyValue(Bytes.toBytes("130"),Bytes.toBytes("f"),Bytes.toBytes("money"),Bytes.toBytes(11111111111L))
          kvList
        }}
      })


    println(kvList)

  }
}
