package com.voyager.flink.connectors.deserialization;


import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaKeyedSerializationSchema implements KafkaSerializationSchema<Tuple2<String, String>> {

    public KafkaKeyedSerializationSchema() {
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        KafkaSerializationSchema.super.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> element, @Nullable Long timestamp) {
        return new ProducerRecord<>(element.f0, element.f1.getBytes(StandardCharsets.UTF_8));
    }
}
