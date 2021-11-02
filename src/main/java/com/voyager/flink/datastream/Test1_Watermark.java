package com.voyager.flink.datastream;

import com.voyager.flink.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.time.Duration;

public class Test1_Watermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> socket = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Event> event = socket.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !StringUtils.isNullOrWhitespaceOnly(value);
            }
        }).map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] words = value.split(",");
                String a0 = words[0];
                String a1 = words[1];
                long a2 = Long.parseLong(words[2]);
                long a3 = Long.parseLong(words[3]);
                return new Event(a0, a1, a2, a3);
            }
        });

        WindowedStream<Event, String, TimeWindow> winStream = event.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getStart();
                            }
                        }
                )
        ).keyBy(Event::getType)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));


        winStream.reduce(new MyReduce(), new MyWindowProcess()).print("watermark");

        env.execute("watermark");


    }

    static class MyReduce implements ReduceFunction<Event> {

        @Override
        public Event reduce(Event value1, Event value2) throws Exception {
            return Math.max(value1.getDuration(), value2.getDuration()) == value1.getDuration() ? value1 : value2;
        }
    }

    static class MyWindowProcess extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Event e = elements.iterator().next();
            out.collect(e.toString());
        }
    }
}
