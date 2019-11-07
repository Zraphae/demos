package cn.com.my.demo.spark

import java.util.UUID

import cn.com.my.demo.spark.utils.{HexStringPartition, ParameterTool}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object App {


  def main(args: Array[String]) {

    // Read parameters from command line
    val params = ParameterTool.fromArgs(args)

//    if (params.getNumberOfParameters() < -1) {
//      println("\nUsage: exportHiveData2HBase " +
//        "--app.name <appName> " +
//        "--hive.table.name <hiveTableName> " +
//        "--hbase.table.name <hbaseTableName> " +
//        "--hive.metastore.uris <hive.metastore.uris> " +
//        "--hbase.zookeeper.quorum <hbase.zookeeper.quorum> " +
//        "--hbase.family.name <hbase.family.name> " +
//        "--hfile.path <hFilePath>")
//      return
//    }

    val hiveMetaStoreUris = params.get("hive.metastore.uris", "thrift://127.0.0.1:9083")
    val appName = params.get("app.name", "exportHiveData2HBase")
    val hiveTableName = params.get("hive.table.name", "employee")
    val hbaseTableName = params.get("hbase.table.name", "person")
    val hFilePath = params.get("hfile.path", "hdfs://pengzhaos-MacBook-Pro.local:9000/hfile")
    val zookeeperQuorum = params.get("hbase.zookeeper.quorum", "127.0.0.1")
    val familyName = params.get("hbase.family.name", "info")

    val stagingFolder = s"${hFilePath}/${hiveTableName}"

    val spark = SparkSession.builder
      .master("local")
      .appName(appName)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", hiveMetaStoreUris)
      .enableHiveSupport()
      .getOrCreate()

    val dataset = spark.sql(s"select * from $hiveTableName")


    hdfsRm(stagingFolder)

    val hConf = HBaseConfiguration.create
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", hbaseTableName)

    val sortedSchema = spark.sql(s"desc ${hiveTableName}").sort("col_name")
    sortedSchema.filter(!_.getString(0).contains("#")).distinct().show()

    import spark.implicits._
    val sortedColName = sortedSchema.map(struct => struct.getString(0)).collect().mkString(",")
    val sortedSchemaWithIdx = sortedSchema.rdd.zipWithIndex()
    val cols = sortedSchemaWithIdx.collect()

    var idKeyIdx = 0;
    for (i <- 1 to (cols.length - 1)) {
      if("id".equals(cols(i)._1.getString(0))){
        idKeyIdx = cols(i)._2.toInt
      }
    }

    val sql = s"select ${sortedColName} from ${hiveTableName}"
    val tableDataSet = spark.sql(sql)

    val defaultFamilyNameBytes = Bytes.toBytes(familyName)
    val defaultPartitions = 10
    val defaultNullStr = "NULL"

    val pairsDataSet = tableDataSet.rdd.map(record => (UUID.randomUUID().toString, record))

    val saltedRDD = pairsDataSet.repartitionAndSortWithinPartitions(new HexStringPartition(defaultPartitions))


    val rdd: RDD[(ImmutableBytesWritable, Seq[KeyValue])] = saltedRDD.map(record => {
      var kvList: Seq[KeyValue] = List()
      val rowKey = Bytes.toBytes(record._1)
      for (i <- 1 to (cols.length - 1)) {
        val colNameBytes = Bytes.toBytes(cols(i)._1.getString(0))
        record._2.getString(cols(i)._2.toInt) match {
          case null => {
            val keyValue = new KeyValue(rowKey, defaultFamilyNameBytes, colNameBytes, Bytes.toBytes(defaultNullStr))
            kvList = kvList :+ keyValue
          }
          case colValue: String => {
            val keyValue = new KeyValue(rowKey, defaultFamilyNameBytes, colNameBytes, Bytes.toBytes(colValue.toString))
            kvList = kvList :+ keyValue
          }
        }
      }
      (new ImmutableBytesWritable(rowKey), kvList)
    })

    val hFileRdd: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.flatMapValues(_.iterator)
    hFileRdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      hConf)

    val conn = ConnectionFactory.createConnection(hConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val load = new LoadIncrementalHFiles(hConf)
    load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(hbaseTableName)))

  }


  def hdfsRm(url: String) {
    val path = new Path(url);
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(url), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(path)) hdfs.delete(path, true)
  }
}
