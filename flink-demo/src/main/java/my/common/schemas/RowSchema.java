package my.common.schemas;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.nio.charset.Charset;


public class RowSchema implements DeserializationSchema<Row>, SerializationSchema<Row> {

    private static final Gson gson = new Gson();

    @Override
    public Row deserialize(byte[] message) {
        return gson.fromJson(new String(message), Row.class);
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Row element) {
        return gson.toJson(element).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}
