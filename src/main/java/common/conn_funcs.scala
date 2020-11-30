package common

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object conn_funcs {

  //连接测试es
  val test_options = Map(
    "es.net.http.auth.user"->"elastic",
    "es.net.http.auth.pass"->"H84I4fw6fDgdenuNRgfe",
    "es.nodes.wan.only" -> "true",
    "es.batch.write.retry.count"->"10",//默认是重试3次,为负值的话为无限重试(慎用)
    "es.batch.write.retry.wait"->"15",//默认重试等待时间是10s.可适当加大
    "es.index.auto.create" -> "true",
    "es.nodes" -> "192.168.18.151:19200,192.168.18.149:19200"
  )

  //连接生产es配置
  val options = Map(
    "es.net.http.auth.user"->"elastic",
    "es.net.http.auth.pass"->"mxtZPtg0VYz2dxa907Oj",
    "es.nodes.wan.only" -> "true",
    "es.batch.write.retry.count"->"10",//默认是重试3次,为负值的话为无限重试(慎用)
    "es.batch.write.retry.wait"->"15",//默认重试等待时间是10s.可适当加大
    "es.index.auto.create" -> "true",
    "es.nodes" -> "10.50.124.155:19200,10.51.78.177:19200,10.50.160.121:19200"
  )


  /**
   *  连接测试hbase, 获取hbase对应表数据
   * @param sc
   * @param tableName
   */
  def get_test_hbase_rdd(sc:SparkContext, tableName:String) = {

    val HbaseConf = HBaseConfiguration.create()

    HbaseConf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
    HbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    HbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)


    //读取hbase数据转换为rdd
     sc.newAPIHadoopRDD(
      HbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
  }


  /**
   *  连接生产hbase, 获取hbase对应表数据
   * @param sc
   * @param tableName
   */
  def get_hbase_rdd(sc:SparkContext, tableName:String) = {

    val HbaseConf = HBaseConfiguration.create()

    HbaseConf.set("hbase.zookeeper.quorum", "10.50.117.228:2181,10.50.213.85:2181,10.50.32.74:2181")
    HbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    HbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)


    //读取hbase数据转换为rdd
    sc.newAPIHadoopRDD(
      HbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
  }


}
