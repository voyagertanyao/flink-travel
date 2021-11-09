package com.voyager.flink.connectors.mysql;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.voyager.flink.connectors.deserialization.KafkaTopicDebeziumDeserializationSchema;
import com.voyager.flink.connectors.deserialization.TableBucketAssigner;
import com.voyager.flink.connectors.deserialization.TopicEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Transform binlog from kafka to hdfs
 */
public class Mysql2Hdfs {
    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
        SourceFunction<Tuple2<String, String>> sourceFunction = MySqlSource.<Tuple2<String, String>>builder()
                .hostname("10.12.21.22")
                .port(3306)
                .databaseList("db4dev") // set captured database
                .tableList("db4dev.t_package_info,db4dev.t_order_table") // set captured table
                .username("root")
                .password("!QAZxsw2")
                .deserializer(new KafkaTopicDebeziumDeserializationSchema())
                .debeziumProperties(debeziumProperties)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60 * 1000);
        env.getCheckpointConfig()
                .setCheckpointStorage("hdfs:///user/flink");

        DataStream<Tuple2<String, String>> logStream = env.addSource(sourceFunction);

        FileSink<Tuple2<String, String>> sink = FileSink.forRowFormat(new Path("hdfs:///user/flink"), new TopicEncoder())
                .withBucketAssigner(new TableBucketAssigner())
                .withRollingPolicy(DefaultRollingPolicy
                        .builder()
                        .withInactivityInterval(TimeUnit.SECONDS.toSeconds(30))
                        .withRolloverInterval(TimeUnit.SECONDS.toSeconds(30))
                        .withMaxPartSize(1024 * 1024 * 128)
                        .build())
                .build();

        logStream.sinkTo(sink).setParallelism(1);

        env.execute();
    }
}
