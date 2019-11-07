package cn.com.my;

import cn.com.my.common.utils.ExecutionEnvUtil;
import cn.com.my.common.utils.GsonUtil;
import cn.com.my.es.ElasticSearchSinkUtil;
import cn.com.my.hbase.HBaseOutputFormat4J;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

import static cn.com.my.common.constant.PropertiesConstants.*;

@Slf4j
public class Main2 {

    private static final String FLINK_CHECKPOINT_PATH = "hdfs://pengzhaos-MacBook-Pro.local:9000/checkpoints-data/";

    public static void main(String[] args) throws Exception {

        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);
//
//        if (params.getNumberOfParameters() < 4) {
//            System.out.println("\nUsage: FlinkReadKafka " +
//                    "--read-topic <topic> " +
//                    "--write-topic <topic> " +
//                    "--bootstrap.servers <kafka brokers> " +
//                    "--group.id <groupid>");
//            return;
//        }
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(params);

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

        env.setStateBackend((StateBackend)new RocksDBStateBackend(FLINK_CHECKPOINT_PATH, true));
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setParallelism(2);

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getConfig()
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10 * 1000L));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // define a schema
        String[] fieldNames = {"id", "num", "ts", "timestampOp"};
        TypeInformation<?>[] dataTypes = {Types.STRING, Types.LONG, Types.STRING, Types.LONG};
        TypeInformation<Row> dataRow = Types.ROW_NAMED(fieldNames, dataTypes);
        TableSchema tableSchema = TableSchema.fromTypeInfo(dataRow);

        tEnv
                .connect(
                        new Kafka()
                                .version("0.11")
                                .property("type", "kafka")
                                .topic("test")
                                .property("bootstrap.servers", "localhost:9092")
                                .property("group.id", "test")
                                /**
                                 * Note that these start position configuration methods do not affect the start position
                                 * when the job is automatically restored from a failure or manually restored using a savepoint.
                                 * On restore, the start position of each Kafka partition is determined by the offsets stored
                                 * in the savepoint or checkpoint.
                                 */
                                .startFromGroupOffsets()
                        )
                .withFormat(
                        new Json()
                                .failOnMissingField(false)
                                .deriveSchema()
                )
                .withSchema(
                        getKafkaSchema(tableSchema)
//                        new Schema()
//                                .field("id", Types.STRING)
//                                .field("num", Types.LONG)
//                                .field("ts", Types.STRING)
//                                .field("timestampOp", Types.LONG)
                )
                .inAppendMode()
                .registerTableSource("kafkaTable");

        Table result = tEnv.sqlQuery("SELECT id, num+1 as num, ts, timestampOp FROM kafkaTable");

        DataStream<Row> rowDataStream = tEnv.toAppendStream(result, dataRow);
        rowDataStream.print();

        HBaseOutputFormat4J hBaseOutputFormat4J = HBaseOutputFormat4J.builder()
                .hbaseZookeeperQuorum("localhost")
                .hbaseZookeeperClientPort("2181")
                .dataRow((RowTypeInfo) dataRow)
                .tableNameStr("test")
                .family("info")
                .rowKeyFiled("id")
                .build();
        rowDataStream.writeUsingOutputFormat(hBaseOutputFormat4J);


        String[] esIndexFields = {"id", "num"};

//        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(params.get(ELASTICSEARCH_HOSTS));
        List<HttpHost> esAddresses = new ArrayList<>();
        esAddresses.add(new HttpHost("127.0.0.1", 9200, "http"));

        int bulkSize = params.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = params.getInt(STREAM_SINK_PARALLELISM, 5);

        log.info("-----esAddresses: {}, parameterTool: {}, ", esAddresses, params);

        ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, rowDataStream,
                (Row record, RuntimeContext runtimeContext, RequestIndexer requestIndexer) ->
                        requestIndexer.add(Requests.indexRequest()
                                .index("test_index")
                                .type("test_type")
                                .source(GsonUtil.toJSONBytes(record, esIndexFields, (RowTypeInfo) dataRow), XContentType.JSON)));

        env.execute("flink demo");
    }

    private static Schema getKafkaSchema(TableSchema tableSchema) {
        Schema schema = new Schema();
        for (int index = 0; index < tableSchema.getFieldCount(); index++) {
            schema.field(tableSchema.getFieldNames()[index], tableSchema.getFieldTypes()[index]);
        }
        return schema;
    }
}