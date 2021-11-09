package com.voyager.flink.connectors.mysql;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.voyager.flink.connectors.deserialization.KafkaKeyedSerializationSchema;
import com.voyager.flink.connectors.deserialization.KafkaTopicDebeziumDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Ingest Mysql binlog to kafka
 * using Flink DataStream API
 */
public class Mysql2Kafka {
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

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "slave003:9092");
//        kafkaProperties.setProperty("acks", "1");
        kafkaProperties.setProperty("auto.create.topics.enable", "true");
        kafkaProperties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60 * 1000);
        env.getCheckpointConfig()
                .setCheckpointStorage("hdfs:///user/flink");

        DataStream<Tuple2<String, String>> logStream = env.addSource(sourceFunction);
//        logStream.map(s->s.f1).print().setParallelism(1);
        logStream.addSink(new FlinkKafkaProducer<>(
                "db4dev",
                new KafkaKeyedSerializationSchema(),
                kafkaProperties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).setParallelism(1);

        env.execute("To Kafka");

    }

}
