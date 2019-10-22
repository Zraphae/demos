package cn.com.my.demo.spark

import java.util.UUID

import cn.com.my.demo.spark.utils.HexStringPartition
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object App {


  def main(args: Array[String]) {


    val spark = SparkSession.builder
      .master("local")
      .appName("test")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate()

    val tableName = "person"
    val hConf = HBaseConfiguration.create
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("hbase.zookeeper.quorum", "127.0.0.1")
    hConf.set("hbase.master", "127.0.0.1:16030")
    hConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)

    val tableDataSet = spark.sql("select * from test.person")
    tableDataSet.show(10)


    val defaultFamilyName = "info"
    val defaultPartitions = 10

    //    import spark.implicits._
    val pairsDataSet = tableDataSet.rdd.map(record => (UUID.randomUUID().toString, record))
    val saltedRDD = pairsDataSet.repartitionAndSortWithinPartitions(new HexStringPartition(defaultPartitions))

    val rdd = saltedRDD.map(r => {
      val rowKey = r._1
      val value = r._2
      val kv: KeyValue = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(defaultFamilyName), Bytes.toBytes("111"), Bytes.toBytes("colName"))
      (new ImmutableBytesWritable(Bytes.add(Bytes.toBytes(rowKey), Bytes.toBytes(r._1))), kv)
    })


    val stagingFolder = "hdfs://pengzhaos-MacBook-Pro.local:9000/hfile/blog.hfile"
    rdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf)

    val conn = ConnectionFactory.createConnection(hConf)
    val table = conn.getTable(TableName.valueOf(tableName))
    val load = new LoadIncrementalHFiles(hConf)
    load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }


  //    import spark.implicits._
  //    val schemaString = "id name tel city"
  //    val fields = schemaString.split(" ")
  //      .map(fieldName => StructField(fieldName, StringType,nullable = true))
  //    val schema = StructType(fields)

  //    val usersDF = spark.read.csv("/Users/zhaopeng/data/test_data/data").toDF("id", "name", "tel",  "city")
  //    usersDF.show()

  //    usersDF.write.format("hive").mode(SaveMode.Append).saveAsTable("test.person")

  // 要保证处于同一个region的数据在同一个partition里面，那么首先我们需要得到table的startkeys
  // 再根据startKey建立一个分区器
  // 分区器有两个关键的方法需要去实现
  // 1. numPartitions 多少个分区
  // 2. getPartition给一个key，返回其应该在的分区  分区器如下：

  //  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
  //    val fraction = 1 max numFilesPerRegion min 128
  //
  //    override def getPartition(key: Any): Int = {
  //      def bytes(n: Any) = n match {
  //        case s: String => Bytes.toBytes(s)
  //        case s: Long => Bytes.toBytes(s)
  //        case s: Int => Bytes.toBytes(s)
  //      }
  //
  //      val h = (key.hashCode() & Int.MaxValue) % fraction
  //      for (i <- 1 until splits.length)
  //        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h
  //
  //      (splits.length - 1) * fraction + h
  //    }
  //
  //    override def numPartitions: Int = splits.length * fraction
  //  }

  // 参数splits为table的startKeys
  // 参数numFilesPerRegion为一个region想要生成多少个hfile，便于理解  先将其设置为1 即一个region生成一个hfile
  // h可以理解为它在这个region中的第几个hfile（当需要一个region有多个hfile的时候）
  // 因为startKeys是递增的，所以找到第一个大于key的region，那么其上一个region，这是这个key所在的region

}
