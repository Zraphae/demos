package cn.com.my.demo.spark

import java.util.Calendar

import cn.com.my.demo.spark.utils.ParameterTool
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{HFileInputFormat, TableInputFormat, TableOutputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, RegionSplitter}
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, HConstants}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


object ReadHBaseSnapshot2Hive {

  case class CellValue(familyName: String, qualifier: String, value: String, timeStamp: Long)

  def main(args: Array[String]) {

    val params = ParameterTool.fromArgs(args)

    val hiveMetaStoreUris = params.get("hive.metastore.uris", "thrift://127.0.0.1:9083")
    val appName = params.get("app.name", "exportHFileData2Hive")
    val hiveTableName = params.get("hive.table.name", "person")
    val nullString = params.get("null.string", "NULL")

    val spark = SparkSession.builder
      .master("local")
      .appName(appName)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", hiveMetaStoreUris)
      .enableHiveSupport()
      .getOrCreate()



    val hConf = HBaseConfiguration.create()
    hConf.set("fs.defaultFS", "hdfs://pengzhaos-MacBook-Pro.local:9000")
    hConf.set("hbase.rootdir", "/hbase")
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("hbase.zookeeper.quorum", "127.0.0.1")
    val scan = new Scan()
    scan.setCacheBlocks(false)
    hConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val job = Job.getInstance(hConf)

    val path = new Path("/tmp")
    val snapName = "person_snapshot"
    TableSnapshotInputFormat.setInput(job, snapName, path, RegionSplitter.HexStringSplit, 8)

    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val record_count = hBaseRDD.count()

    println(s"------>$record_count")


  }


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBase64String(proto.toByteArray());
  }
}
