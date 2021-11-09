package com.voyager.flink.connectors.deserialization;

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

    private boolean snapshot = false;

    public CustomDebeziumDeserializationSchema(boolean snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        Struct source = ((Struct) record.value()).getStruct("source");
        String snapshot = source.getString("snapshot");
        if ("true".equals(snapshot) || "last".equals(snapshot)) {
            out.collect(extractSnapshotRow(record));
            if (this.snapshot && "last".equals(snapshot)) {
                System.out.println("BINLOG: " + source.get("file") + "/" + source.get("pos"));
            }
            return;
        }
        Envelope.Operation op = Envelope.operationFor(record);
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE || op == Envelope.Operation.TRUNCATE) {
                out.collect(extractDeleteRow(record));
            } else {
                out.collect(extractUpdateRow(record));
            }
        } else {
            out.collect(extractInsertRow(record));
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    private String extractInsertRow(SourceRecord record) throws Exception {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("after");
        return getString(offset, after, "INSERT");
    }

    private String extractDeleteRow(SourceRecord record) throws Exception {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("before");
        return getString(offset, after, "DELETE");
    }

    private String extractUpdateRow(SourceRecord record) throws Exception {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("after");
        return getString(offset, after, "UPDATE");
    }

    private String extractSnapshotRow(SourceRecord record) throws Exception {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("after");
        return getString(offset, after, "SNAPSHOT");
    }

    private String getString(Map<String, ?> offset, Struct record, String op) {
        JSONObject json = new JSONObject();
        json.put("_sec", System.nanoTime());
        json.put("_pos", offset.get("pos"));
        json.put("_op", op);
        for (Field field : record.schema().fields()) {
            json.put(field.name(), record.get(field));
        }
        return json.toJSONString();
    }
}
