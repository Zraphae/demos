package my;

import cn.com.my.common.model.MetricEvent;
import cn.com.my.common.schemas.MetricSchema;
import cn.com.my.common.utils.ExecutionEnvUtil;
import cn.com.my.common.utils.KafkaConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {

        // Read parameters from command line
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        if(params.getNumberOfParameters() < 4) {
//            log.error("Usage: FlinkReadKafka " +
//                    "--read-topic <topic> " +
//                    "--write-topic <topic> " +
//                    "--bootstrap.servers <kafka brokers> " +
//                    "--group.id <groupid>");
//            return;
//        }

        final ParameterTool params = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(params);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStreamSource<MetricEvent> kafkaStream = KafkaConfigUtil.buildSource(env, new MetricSchema());

        // convert DataStream to Table
//        Table kafkaTable = tEnv.fromDataStream(kafkaStream, "id, num, ts, timestampOp");

        // register DataStream as Table
        tEnv.registerDataStream("kafkaTable", kafkaStream);
        Table result = tEnv.sqlQuery("SELECT id, num+1 as num, ts, timestampOp FROM kafkaTable");

        tEnv.toAppendStream(result, MetricEvent.class).print();

        env.execute("flink demo");
    }

}
