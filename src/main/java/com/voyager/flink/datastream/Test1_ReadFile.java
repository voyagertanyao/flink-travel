package com.voyager.flink.datastream;

import com.voyager.flink.bean.Device;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test1_ReadFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);


        DataStream<String> txt = env.readTextFile("/Users/tanyao/IdeaProjects/flink-travel/src/main/resources/data.txt");

        SingleOutputStreamOperator<Device> mapStream = txt.map(data -> {
            String[] columns = data.split(",");
            return new Device(columns[0], columns[1], columns[2]);
        });

        SingleOutputStreamOperator<Device> filterStream = mapStream.filter(device -> "2021-03-26".equals(device.getSendDate()));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = filterStream
                .map(new MapFunction<Device, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Device device) throws Exception {
                        return Tuple2.of(device.getModelName(), 1);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        sum.print();

        mapStream.map(new MapFunction<Device, Integer>() {
            @Override
            public Integer map(Device value) throws Exception {
                return 1;
            }
        }).keyBy(t -> t).sum(0).print("total");

        env.execute();
    }
}
