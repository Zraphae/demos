package my.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Objects;

@Slf4j
public class HBaseSink4J extends RichSinkFunction<Row> {

    private Connection conn;
    private Table table;
    private BufferedMutator bufferedMutator;
    private TypeInformation<Row> dataRow;

    public HBaseSink4J(Configuration configuration, TypeInformation<Row> dataRow) throws IOException {
        this.conn = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf("test");
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);

        params.writeBufferSize(1024 * 1024);
        this.bufferedMutator = conn.getBufferedMutator(params);

        this.table = this.conn.getTable(tableName);
        this.dataRow = dataRow;
    }


    @Override
    public void invoke(Row value, Context context) throws Exception {

        log.info("=======>{}", value);
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (!Objects.isNull(this.bufferedMutator)) {
            this.bufferedMutator.flush();
            this.bufferedMutator.close();
        }
        if (!Objects.isNull(this.table)) this.table.close();
        if (!Objects.isNull(this.conn)) this.conn.close();
    }
}
