package com.voyager.flink.connectors.mysql;


import com.ververica.cdc.connectors.mysql.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class MysqlConnector {
    public static void main(String[] args) throws Exception {

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("10.12.21.22")
                .port(3306)
                .databaseList("flink-dev") // set captured database
                .tableList("flink-dev.orders") // set captured table
                .username("root")
                .password("!QAZxsw2")
                .deserializer(new CustomDebeziumDeserializationSchema()) // converts SourceRecord to String
                .debeziumProperties(debeziumProperties)
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.enableCheckpointing(3000); // checkpoint every 3000 milliseconds
        env.addSource(sourceFunction).print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
        env.execute();
    }
}
