package my.hbase

import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[String] {


  var conn: Connection = null
  var table: Table = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost")
    conn = ConnectionFactory.createConnection(conf)
    table = conn.getTable(TableName.valueOf(tableName))
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    val g = new Gson()
    val student = g.fromJson(value, classOf[Student])

    val put: Put = new Put(Bytes.toBytes(student.sid))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(student.name))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("age"), Bytes.toBytes(student.age))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("sex"), Bytes.toBytes(student.sex))
    table.put(put)
  }

  override def close(): Unit = {
    super.close()
    if(table != null){
      table.close()
    }
    if(conn != null){
      conn.close()
    }

  }
}

case class Student(name: String, age: Int, sex: String, sid: String)