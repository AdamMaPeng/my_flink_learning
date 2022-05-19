package com.atguigu.flink.chapter08;

/**
 * @author Adam-Ma
 * @date 2022/5/10 16:56
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 *  coGroup 窗口同组联结
 *     加强版的 window Join ，可以实现除 内联接之外的其他联结操作
 *
 *     用法同 window join ,
 *        stream1
 *          .coGroup(stream2)
 *          .where(key)
 *          .equalTo()
 *          .apply()
 *
 */
public class Test06_coGroup {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 数据源1 ：
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L),
                Tuple3.of("Adam", "order-6", 51000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );

        // 数据源2
        SingleOutputStreamOperator<Event> stream2 = env.fromElements(
                new Event("Bob", "./cart", 20000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 36000L),
                new Event("Bob", "./home", 30000L),
                new Event("Bob", "./prod?id=1", 23000L),
                new Event("Bob", "./prod?id=3", 33000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // coGroup ，获取同一用户，下单和 页面浏览的所有记录，
        stream1
                .coGroup(stream2)
                .where(tuple3 -> tuple3.f0)
                .equalTo(event -> event.user)
                .window(
                        TumblingEventTimeWindows.of(Time.seconds(10))
                )
                .apply(new CoGroupFunction<Tuple3<String, String, Long>, Event, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Long>> first, Iterable<Event> second, Collector<String> out) throws Exception {
                        out.collect(first + " => " + second);
                    }
                }).print("Result : ");

        // 环境执行
        env.execute();
    }
}

