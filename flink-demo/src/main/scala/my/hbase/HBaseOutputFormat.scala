package my.hbase

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HBaseOutputFormat[Order] extends OutputFormat[Row]{

  val zkServer = "localhost"
  val port = "2181"
  var conn: Connection = null
//  var mutator: BufferedMutator = null
//  var count = 0
  var table: Table = null

  /**
    * 配置输出格式。此方法总是在实例化输出格式上首先调用的
    *
    * @param configuration
    */
  override def configure(configuration: Configuration): Unit = {

  }

  /**
    * 用于打开输出格式的并行实例，所以在open方法中我们会进行hbase的连接，配置，建表等操作。
    *
    * @param i
    * @param i1
    */
  override def open(i: Int, i1: Int): Unit = {
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    config.set(HConstants.ZOOKEEPER_QUORUM, zkServer)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
    conn = ConnectionFactory.createConnection(config)

    val tableName: TableName = TableName.valueOf("test")

//    val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
//    //设置缓存1m，当达到1m时数据会自动刷到hbase
//    params.writeBufferSize(1024 * 1024) //设置缓存的大小
//    mutator = conn.getBufferedMutator(params)
//    count = 0

    table = conn.getTable(tableName)
  }

  /**
    * 用于将数据写入数据源，所以我们会在这个方法中调用写入hbase的API
    *
    * @param it
    */
  override def writeRecord(it: Row): Unit = {

    print(it)
    val info = "info"
    val put: Put = new Put(Bytes.toBytes(it.getField(0).asInstanceOf[String]))
    put.addColumn(Bytes.toBytes(info), Bytes.toBytes("name"), Bytes.toBytes(it.getField(1).asInstanceOf[Long]))
    put.addColumn(Bytes.toBytes(info), Bytes.toBytes("age"), Bytes.toBytes(it.getField(2).asInstanceOf[String]))

    table.put(put)
//    mutator.mutate(put)
//    //每4条刷新一下数据，如果是批处理调用outputFormat，这里填写的4必须不能大于批处理的记录总数量，否则数据不会更新到hbase里面
//    if (count >= 4){
//      mutator.flush()
//      count = 0
//    }
//    count = count + 1
  }

  /**
    * 关闭
    */
  override def close(): Unit = {
    try {
      if (conn != null) conn.close()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}
