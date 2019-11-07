package cn.com.my.hbase;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Objects;

@Slf4j
@Builder
public class HBaseOutputFormat4J implements OutputFormat<Row> {


    private String rowKeyFiled;
    private String tableNameStr;
    private String family;
    private RowTypeInfo dataRow;
    private String hbaseZookeeperQuorum;
    private String hbaseZookeeperClientPort;
    private int taskNumber;
    private int numTasks;
    private Connection conn;
    private Table table;
    private BufferedMutator bufferedMutator;
    private org.apache.hadoop.conf.Configuration configuration;

    private static final long serialVersionUID = 1L;


    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        this.taskNumber = taskNumber;
        this.numTasks = numTasks;

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", this.hbaseZookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", this.hbaseZookeeperClientPort);
        this.conn = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf(this.tableNameStr);

        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        params.writeBufferSize(1024 * 1024);
        this.bufferedMutator = conn.getBufferedMutator(params);

        this.table = this.conn.getTable(tableName);

    }

    @Override
    public void writeRecord(Row record) throws IOException {

        if (Objects.isNull(this.dataRow) || this.dataRow.getArity() != record.getArity()) {
            log.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
            return;
        }

        int idIndex = this.dataRow.getFieldIndex(this.rowKeyFiled);
        Object idFiled = record.getField(idIndex);

        Put put = new Put(Bytes.toBytes(idFiled.toString()));
        for (int index = 0; index < this.dataRow.getArity(); index++) {

            Class<?> typeClass = this.dataRow.getFieldTypes()[index].getTypeClass();
            String key = this.dataRow.getFieldNames()[index];
            Object value = record.getField(index);
            Object type = typeClass.cast(value);

            if (log.isDebugEnabled()) {
                log.info("key: {}, value: {}, type: {}", key, value, typeClass);
            }
            if (!Objects.isNull(value)) {
                if (type instanceof Integer) {
                    put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((int) value));
                } else if (type instanceof Long) {
                    put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((long) value));
                } else if (type instanceof Float) {
                    put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((float) value));
                } else if (type instanceof Double){
                    put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((double) value));
                } else if (type instanceof String) {
                    put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((String) value));
                } else {
                    String errorMessage = String.format(
                            "field index: %s, field value: %s, field type: %s.", index, key, type);
                    throw new ClassCastException(errorMessage);
                }
            }

        }

        this.table.put(put);
    }

    @Override
    public void close() throws IOException {
        if (!Objects.isNull(this.bufferedMutator)) {
            this.bufferedMutator.flush();
            this.bufferedMutator.close();
        }
        if (!Objects.isNull(this.table)) this.table.close();
        if (!Objects.isNull(this.conn)) this.conn.close();
    }


}