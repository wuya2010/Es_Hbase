package common

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object conn_funcs {

  /**
   *  连接hbase, 获取hbase对应表数据
   * @param sc
   * @param tableName
   */
  def get_hbase_rdd(sc:SparkContext, tableName:String) = {

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


}
