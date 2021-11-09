package com.voyager.flink.connectors.deserialization;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class KafkaTopicDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Tuple2<String, String>> {

    public KafkaTopicDebeziumDeserializationSchema() {
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple2<String, String>> collector) throws Exception {
        Struct source = ((Struct) sourceRecord.value()).getStruct("source");
        String db = source.getString("db");
        String table = source.getString("table");
//        long pos = source.getInt64("pos");
        String key = String.join(".", db, table);
        String jsonStr = convert(sourceRecord);
        collector.collect(Tuple2.of(key, jsonStr));
    }

    private String convert(SourceRecord record) {
        Envelope.Operation op = Envelope.operationFor(record);
        String result = "";
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE || op == Envelope.Operation.TRUNCATE) {
                result = extractDeleteRow(record);
            } else {
                result = extractUpdateRow(record);
            }
        } else {
            result = extractInsertRow(record);
        }
        return result;

    }

    private String extractInsertRow(SourceRecord record) {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("after");
        return getString(offset, after, "INSERT");
    }

    private String extractDeleteRow(SourceRecord record) {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("before");
        return getString(offset, after, "DELETE");
    }

    private String extractUpdateRow(SourceRecord record) {
        Map<String, ?> offset = record.sourceOffset();
        Struct after = ((Struct) record.value()).getStruct("after");
        return getString(offset, after, "UPDATE");
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

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }
}
