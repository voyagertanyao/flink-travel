package com.voyager.flink.connectors.mysql;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.StringUtils;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class MysqlConnector {
    public static void main(String[] args) throws Exception {

        final char delimiter = '\007';

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String file = parameterTool.get("file", "");
        int pos = parameterTool.getInt("pos", 0);

        StartupOptions initial = StartupOptions.initial();
        StartupOptions special = StartupOptions.specificOffset(file, pos);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("10.12.21.22")
                .port(3306)
                .databaseList("db4dev") // set captured database
                .tableList("db4dev.t_package_info") // set captured table
                .username("root")
                .password("!QAZxsw2")
                .deserializer(new CustomDebeziumDeserializationSchema(StringUtils.isNullOrWhitespaceOnly(file)))
                .debeziumProperties(debeziumProperties)
                .startupOptions(StringUtils.isNullOrWhitespaceOnly(file) ? initial : special)
                .build();

        Path path = new Path("hdfs:///user/flink/db4_dev/ods_package_info_delta");
        final FileSink<String> sink = FileSink.forRowFormat(path, new SimpleStringEncoder<String>("utf-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(30))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(30))
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build())
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.enableCheckpointing(1 * 1000);
        executionEnvironment.getCheckpointConfig()
                .setCheckpointStorage("hdfs:///user/flink/db4_dev/ods_package_info_delta/checkpoint");

        DataStream<String> rowDataDataStreamSource = executionEnvironment.addSource(sourceFunction);
        rowDataDataStreamSource.sinkTo(sink).setParallelism(1);

        executionEnvironment.execute();
    }
}
