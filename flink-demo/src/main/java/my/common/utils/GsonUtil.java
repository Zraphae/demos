package my.common.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.nio.charset.Charset;
import java.util.Objects;


@Slf4j
public class GsonUtil {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private final static Gson gson = new Gson();

    public static <T> T fromJson(String value, Class<T> type) {
        return gson.fromJson(value, type);
    }

    public static String toJson(Object value) {
        return gson.toJson(value);
    }

    public static byte[] toJSONBytes(Object value) {
        return gson.toJson(value).getBytes(DEFAULT_CHARSET);
    }

    public static byte[] toJSONBytes(Row record, String[] esIndexFields, RowTypeInfo rowTypeInfo) {

        if (Objects.isNull(esIndexFields) || esIndexFields.length == 0) {
            return "".getBytes(DEFAULT_CHARSET);
        }

        JsonObject result = new JsonObject();
        for (int index = 0; index < esIndexFields.length; index++) {

            String key = esIndexFields[index];
            int fieldIndex = rowTypeInfo.getFieldIndex(key);
            TypeInformation<?> fieldType = rowTypeInfo.getFieldTypes()[fieldIndex];
            Class<?> typeClass = fieldType.getTypeClass();
            Object value = record.getField(fieldIndex);

            Object type = typeClass.cast(value);

            if(log.isDebugEnabled()){
                log.debug("key: {}, fieldIndex: {}, value: {}", key, fieldIndex, value);
            }

            if (type instanceof Integer) {
                result.addProperty(key, (int) value);
            } else if (type instanceof Long) {
                result.addProperty(key, (long) value);
            } else if (type instanceof Float) {
                result.addProperty(key, (float) value);
            } else if (type instanceof Double) {
                result.addProperty(key, (double) value);
            } else if (type instanceof String) {
                result.addProperty(key, (String) value);
            } else {
                String errorMessage = String.format(
                        "field index: %s, field value: %s, field type: %s.", index, key, type);
                throw new ClassCastException(errorMessage);
            }

        }
        return result.toString().getBytes(DEFAULT_CHARSET);
    }
}
