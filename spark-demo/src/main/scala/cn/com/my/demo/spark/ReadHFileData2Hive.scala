package cn.com.my.demo.spark


import cn.com.my.demo.spark.utils.ParameterTool
import org.apache.hadoop.hbase.mapreduce.HFileInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


object ReadHFileData2Hive {

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

    val sortedSchema = spark.sql(s"desc ${hiveTableName}").sort("col_name")

    import spark.implicits._
    val cols = sortedSchema.map(struct => struct.getString(0)).collect()
    val fields = sortedSchema.collect().map(hiveSchema => StructField(hiveSchema.getString(0), StringType, nullable = true))
    val schema = StructType(fields)

    val stagingFolder = "/Users/zhaopeng/Data/testDataSet/hbase_snapshot/archive/data/default/person/*"
    val hfileRdd: RDD[(NullWritable, Cell)] = spark.sparkContext.newAPIHadoopFile[NullWritable, Cell, HFileInputFormat](stagingFolder)
    val cellValueRdd: RDD[(String, Iterable[CellValue])] = hfileRdd.values.map(cell => {
      val family = Bytes.toString(CellUtil.cloneFamily(cell))
      val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      val timeStamp = cell.getTimestamp
      (Bytes.toString(CellUtil.cloneRow(cell)), CellValue(family, qualifier, value, timeStamp))
    }).groupByKey()

    val cellValueMapRdd = cellValueRdd.map(pairs => {
      var keyValues = mutable.HashMap.empty[String, CellValue]
      pairs._2.foreach(cellValue => {
        if (keyValues contains cellValue.qualifier) {
          val containedCellValue = keyValues(cellValue.qualifier)
          if (cellValue.timeStamp > containedCellValue.timeStamp) {
            keyValues += (cellValue.qualifier -> cellValue)
          }
        } else {
          keyValues += (cellValue.qualifier -> cellValue)
        }
      })
      (pairs._1, keyValues)
    })

    val valueRdd = cellValueMapRdd.map(pairs => {
      var row = mutable.ListBuffer.empty[String]
      cols.foreach(qualifier => {
        if (!pairs._2.contains(qualifier)) {
          row += nullString
        } else {
          row += pairs._2(qualifier).value
        }
      })
      Row.fromSeq(row)
    })
    spark.createDataFrame(valueRdd, schema).write.format("hive").insertInto(hiveTableName)
  }
}
