package com.voyager.flink.connectors.deserialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class TableBucketAssigner implements BucketAssigner<Tuple2<String, String>, String> {


    public TableBucketAssigner() {
    }

    @Override
    public String getBucketId(Tuple2<String, String> element, Context context) {
        return element.f0;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
