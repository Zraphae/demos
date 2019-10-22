//package cn.com.my
//
//
//import java.net.{InetAddress, InetSocketAddress}
//
//import cn.com.my.hbase.HBaseOutputFormat
//import org.apache.flink.api.scala._
//import org.apache.flink.api.common.functions.RuntimeContext
//import org.apache.flink.api.scala.typeutils.Types
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.apache.flink.table.api.Table
//import org.apache.flink.table.api.scala._
//import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
//import org.apache.flink.types.Row
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//
//
//
//object App {
//
//  private val LOCAL_ZOOKEEPER_HOST = "localhost:2181"
//  private val LOCAL_KAFKA_BROKER = "localhost:9092"
//  private val RIDE_SPEED_GROUP = "rideSpeedGroup"
//  private val RIDE_SPEED_TOPIC = "test"
//
//  def main(args: Array[String]) {
//
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = StreamTableEnvironment.create(env)
//
//
//    tableEnv.connect(new Kafka()
//      .version("0.11")
//      .topic(RIDE_SPEED_TOPIC)
//      .property("bootstrap.servers", LOCAL_KAFKA_BROKER))
//      .withSchema(
//        new Schema()
//        .field("id", Types.STRING)
//        .field("num", Types.LONG)
//        .field("ts", Types.STRING)
////        .rowtime(new Rowtime()
////          .timestampsFromSource()
////          .watermarksPeriodicBounded(1000)
////        )
//      )
//      .withFormat(new Json().deriveSchema())
//      .inAppendMode()
//      .registerTableSource("kafkaTable")
//
//    val kafkaTable: Table = tableEnv.sqlQuery("select id, num + 1, ts from kafkaTable")
//
//    kafkaTable.toAppendStream[Order].print()
//
//    kafkaTable.writeUsingOutputFormat(new HBaseOutputFormat)
//
//    val config = new java.util.HashMap[String, String]
//    config.put("cluster.name", "my-cluster-name")
//    config.put("bulk.flush.max.actions", "1")
//
//    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
//    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300))
//
//    kafkaTable.addSink(new ElasticsearchSink[Row](config, transportAddresses, new ElasticsearchSinkFunction[Row] {
//      def createIndexRequest(element: String): IndexRequest = {
//        print(element)
//        val json = new java.util.HashMap[String, String]
//        json.put("data", element)
//
//        return Requests.indexRequest()
//          .index("my-index")
//          .`type`("my-type")
//          .source(json)
//      }
//
//      def process(element: Row, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        indexer.add(createIndexRequest(element.getField(0).toString))
//      }
//    }))
//
//    env.execute("flink demo")
//  }
//
//}
//
//case class Order(id: String, num: Long, ts: String)