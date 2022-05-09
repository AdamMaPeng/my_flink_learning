package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/8 20:17
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.collection.mutable.HashSet;

import java.time.Duration;

/**
 *   通过 窗口中增量聚合函数，完成 PV UV 的统计
 *      PV : page view
 *      UV : Unique View
 *
 *   需求： 得到 PV / UV , 每个用户平均点击了多少网页
 */
public class Test05_WindowAggregatePvUv {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取数据源
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 给数据源中的每条数据 设置 水位线
        SingleOutputStreamOperator<Event> streamWithWM = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // 将数据进行分区
        KeyedStream<Event, String> keyedStream = streamWithWM.keyBy(data -> "Visit");

        // 开启窗口
        keyedStream.window(
                SlidingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(2))
        )
                .aggregate(new AggregateFunction<Event, Tuple2
                        <HashSet<String>, Long>, Double>() {
                    @Override
                    public Tuple2<HashSet<String>, Long> createAccumulator() {
                        HashSet<String> set = new HashSet<>();
                        return Tuple2.of(set, 0L);
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
                        accumulator.f0.add(value.user);   // 返回 boolean, 不能直接加到累加器中
                        return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
                    }

                    @Override
                    public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                        return (double) (accumulator.f1 / accumulator.f0.size());
                    }

                    @Override
                    public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                        return null;
                    }
                })
                .print("data");



        // 环境执行
        env.execute();
    }
}
