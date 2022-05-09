package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/7 15:59
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 *  测试如何生成水位线：
 *      WaterMarks
 *
 *      有序流：（只会在测试中使用，真实的项目中，不会使用有序流的生成水位线方式的）
 *             stream.assignTimestampsAndWatermarks(
 *                  WatermarkStrategy.forMonotonousTimestramps()
 *                  .withTimestampAssigner(
 *                     new SerializableTimestampAssigner<Event>() {
 * //                    @Override
 * //                    public long extractTimestamp(Event element, long recordTimestamp) {
 * //                        return element.timestamp;
 * //                    }
 *                      }
 *                  )
 *             )
 *      乱序流：
 *          stream.assignTimestampsAndWatermarks(
 *              WatermarkStrategy.forBoundOutOfOrderness()
 *          )
 */
public class Test01_GenerWatermarks {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 周期性设置水位线默认 200ms ，时间太久，可以设置为 100 ms
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源
        DataStream<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        // 有序：只测试使用， 有序流的 watermark 生成 ： monotonous 单调的，非乱序的
//        stream.assignTimestampsAndWatermarks(
//                // 参数为 WatermarkStrategy ，对于 有序的数据流，指定泛型调用 forMonotonousTimestamps 返回 WatermarkStrategy
//                WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        // 有了水位线，同时还要指定时间戳
//                .withTimestampAssigner(
//                  new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event element, long recordTimestamp) {
//                        return element.timestamp;
//                    }
//                })
//        );

        // 乱序流的 watermark 生成
        stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                        Duration.ofMillis(100L)
                )
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }
                )
        );

        // 执行环境
        env.execute();
    }
}
