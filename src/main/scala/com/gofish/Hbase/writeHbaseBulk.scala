package com.gofish.Hbase

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2020/12/22 10:42
 */
object writeHbaseBulk {

  val zkCluster = "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181"
  val hbasePort = "2181"
  val tableName = TableName.valueOf("test_wang")
  val columnFamily = Bytes.toBytes("info")

  case class testHbase(rowkey:String,name:String,id:Int,money:Long)


  def main(args: Array[String]): Unit = {

    //1.0 设置 sparkConf
    val conf = new SparkConf().setAppName("Hfile").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

//    conf.registerKryoClasses(Array(classOf[D_class]))

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
//    val fileData: RDD[String] = sc.textFile("data")


    //将rdd 写入hbase
    import spark.implicits._
    val test_df = sc.parallelize(Array(
      testHbase("110", " aabc  -- 学生 ——？",1,11111111111L),
      testHbase("120", "woshi 老师 & 学校",1,22222222222L),
      testHbase("130", "nihao ---主页",1,333333333L)
    )).toDF().rdd


    //这个也很麻烦啊
    val get_rdd = test_df.map{row =>
      val key  = row.getAs[String]("rowkey")
      val rowkey =new ImmutableBytesWritable(Bytes.toBytes(row.getAs[String]("rowkey")))
      val name  = row.getAs[String]("name")
      val money  = row.getAs[String]("money")

      val value = new KeyValue(Bytes.toBytes(key),columnFamily,columnFamily,Bytes.toBytes(name+":"+money))
      //key是什么类型， value 是另外类型
      (rowkey,value)
    }


    hfile_load(get_rdd)


//    //rdd转换
//    // 以rowkey 作为key , 其他作为 value  fixme:是否有排序
//    val rdd = fileData.map{
//      line =>
//        val dataArray = line.split("@")
//        val rowkey = dataArray(0)+dataArray(1)
//        val ik = new ImmutableBytesWritable(Bytes.toBytes(rowkey))
//        val kv = new KeyValue(Bytes.toBytes(rowkey) , columnFamily ,  Bytes.toBytes("info") , Bytes.toBytes(dataArray(2)+":"+dataArray(3)+":"+dataArray(4)+":"+dataArray(5)))
//        (ik , kv)
//    }


    //    val scheduledThreadPool = Executors.newScheduledThreadPool(4);
    //    scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
    //      override def run(): Unit = {
    //        println("=====================================")
    //      }
    //
    //    }, 1, 3, TimeUnit.SECONDS);
//    hfile_load(rdd)
  }

  def hfile_load(rdd:RDD[Tuple2[ImmutableBytesWritable , KeyValue]]): Unit ={
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181")
//    hconf.set("hbase.master", "hadoop01:60000")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.rpc.timeout", 20000)
    hconf.setInt("hbase.client.operation.timeout", 30000)
    hconf.setInt("hbase.client.scanner.timeout.period", 200000)
//    hconf.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,100)//你要设置的Hfile个数
    //声明表的信息
    var table: Table = null
    var connection: Connection = null


    try{
      val startTime = System.currentTimeMillis()
      println(s"开始时间：-------->${startTime}")


      //生成的HFile的临时保存路径
      val stagingFolder = "hdfs://192.168.18.148:8020/hfile4"

      //开始即那个HFile导入到Hbase,此处都是hbase的api操作
      val load = new LoadIncrementalHFiles(hconf)




      //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
      connection = ConnectionFactory.createConnection(hconf)
      //根据表名获取表
      table = connection.getTable(tableName)
      val admin = connection.getAdmin
      //构造表描述器
      val hTableDescriptor = new HTableDescriptor(tableName)
      //构造列族描述器
      val hColumnDescriptor = new HColumnDescriptor(columnFamily)
      hTableDescriptor.addFamily(hColumnDescriptor)
      //如果表不存在，则创建表
      if(!admin.tableExists(tableName)){
        admin.createTable(hTableDescriptor)
      }

      //获取hbase表的region分布
      val regionLocator = connection.getRegionLocator(tableName)
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(hconf)
      //设置job名称
      job.setJobName("DumpFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFile,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

      //将日志保存到指定目录
      rdd.saveAsNewAPIHadoopFile(stagingFolder,
        classOf[ImmutableBytesWritable],
        classOf[KeyValue],
        classOf[HFileOutputFormat2],
        hconf)




      //开始导入
      load.doBulkLoad(new Path(stagingFolder),admin,table.asInstanceOf[HTable],regionLocator)
      //传入所有参数
//      load.doBulkLoad(new Path(stagingFolder), table.asInstanceOf[HTable])

      //查看生成多少个 hadoop fs -count /tmp/hbaeTest  hdfs存储路径： /tmp/hbaeTest

      val endTime = System.currentTimeMillis()
      println(s"结束时间：-------->${endTime}")
      println(s"花费的时间：----------------->${(endTime - startTime)}")
    }catch{
      case e:IOException =>
        e.printStackTrace()
    }finally {
      if (table != null) {
        try {
          // 关闭HTable对象 table.close();
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
      if (connection != null) {
        try { //关闭hbase连接. connection.close();
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
    }

  }


}
