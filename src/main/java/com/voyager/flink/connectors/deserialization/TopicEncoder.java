package com.voyager.flink.connectors.deserialization;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class TopicEncoder implements Encoder<Tuple2<String, String>> {

    public TopicEncoder() {
    }

    @Override
    public void encode(Tuple2<String, String> element, OutputStream stream) throws IOException {
        stream.write(element.f1.getBytes(StandardCharsets.UTF_8));
        stream.write('\n');
    }
}
