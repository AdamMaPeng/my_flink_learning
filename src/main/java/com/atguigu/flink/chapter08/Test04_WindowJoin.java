package com.atguigu.flink.chapter08;

/**
 * @author Adam-Ma
 * @date 2022/5/10 15:15
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *  基于时间的合流 -- 双流联结（Join ）
 *
 *  窗口联结（Window Join）
 *      stream1.join(stream2)
 *             .where(<KeySelector>)
 *             .equalTo(<KeySelector>)
 *             .window(<WindowAssigner>)
 *             .apply(<JoinFunction>)
 */
public class Test04_WindowJoin {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("a", 5200L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("a", 3000),
                Tuple2.of("b", 3000),
                Tuple2.of("a", 4000),
                Tuple2.of("b", 4000),
                Tuple2.of("a", 5500)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
        );

        // 将两个流进行 窗口联结（Window Join）
        DataStream<String> joinedStream = stream1.join(stream2)
                .where(stream1_Tuple2 -> stream1_Tuple2.f0)
                .equalTo(stream2_Tuple2 -> stream2_Tuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> first, Tuple2<String, Integer> second) throws Exception {
                        return "Stream_1: " + first + " & Stream_2:" + second;
                    }
                });

        joinedStream.print("窗口联结 Join : ");

        // 环境执行
        env.execute();
    }
}
