package com.voyager.flink.connectors.mysql;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = -3168848963265670603L;

    private final char DELIMITER = '|';

    private final String NULL = "\\N";

    public CustomDebeziumDeserializationSchema() {
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE) {
                out.collect(extractBeforeRow(record));
            } else {
                out.collect(extractBeforeRow(record));
                out.collect(extractAfterRow(record));
            }
        } else {
            out.collect(extractAfterRow(record));
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    private String extractAfterRow(SourceRecord record) throws Exception {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("after");
        return getString(offset, after, "INSERT");
    }

    private String extractBeforeRow(SourceRecord record) throws Exception {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("before");
        return getString(offset, after, "DELETE");
    }

    private String getString(Map<String, ?> offset, Struct record, String op) {
        JSONObject json = new JSONObject();
        json.put("_sec", offset.get("ts_sec"));
        json.put("_pos", offset.get("pos"));
        json.put("_op", op);
        for (Field field : record.schema().fields()) {
            json.put(field.name(), record.get(field));
        }
        return json.toJSONString();
    }
}
