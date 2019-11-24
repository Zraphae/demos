package cn.com.my.demo.spark


import cn.com.my.demo.spark.utils.ParameterTool
import cn.com.my.spark.utils.MyHexRegionSplit
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
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
    TableSnapshotInputFormat.setInput(job, snapName, path, new MyHexRegionSplit, 2)
    val hBaseRDD  = spark.sparkContext.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    val schema = spark.table(hiveTableName).schema
    spark.sparkContext.broadcast(schema)

    val valueRdd = hBaseRDD.values.map(result => {
      var row = mutable.ListBuffer.empty[String]
      val familyNameBytes = Bytes.toBytes("info")
      schema.foreach(structField => {
        val value = result.getValue(familyNameBytes, Bytes.toBytes(structField.name))
        if(null == value){
          row += nullString
        }else{
          row += Bytes.toString(value)
        }
      })
      Row.fromSeq(row)
    })

    spark.createDataFrame(valueRdd, schema).write.format("hive").insertInto("test:test_hive")

  }


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBase64String(proto.toByteArray());
  }
}
