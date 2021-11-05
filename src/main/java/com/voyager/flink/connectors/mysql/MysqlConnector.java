package com.voyager.flink.connectors.mysql;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class MysqlConnector {
    public static void main(String[] args) throws Exception {

        final char delimiter = '\007';

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("10.12.21.22")
                .port(3306)
                .databaseList("db4dev") // set captured database
                .tableList("db4dev.t_package_info") // set captured table
                .username("root")
                .password("!QAZxsw2")
                .deserializer(new CustomDebeziumDeserializationSchema())
                .debeziumProperties(debeziumProperties)
                .build();


        Path path = new Path("hdfs:///user/flink/db4dev/t_package_info");
        final FileSink<String> sink = FileSink.forRowFormat(path, new SimpleStringEncoder<String>("utf-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(60))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(60))
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build())
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(60 * 1000);
        executionEnvironment.getCheckpointConfig()
                .setCheckpointStorage("hdfs:///user/flink");

        DataStream<String> rowDataDataStreamSource = executionEnvironment.addSource(sourceFunction);
        rowDataDataStreamSource.sinkTo(sink).setParallelism(1);

        executionEnvironment.execute();
    }
}
