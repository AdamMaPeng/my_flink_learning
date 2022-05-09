package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/8 12:05
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.operators.window.CountWindow;

import java.time.Duration;

/**
 *  窗口 API 测试：
 *       stream.
 *                     .countWindow(long size[, long slide]) : [滚动/滑动]计数窗口
 *                     .TumblingEventTimeWindow(long size)   :  滚动事件时间窗口
 *                     .SlidingEventTimeWindow()             :  滑动事件时间窗口
 *                     .EventTimeSessionWindow()             :  事件时间会话窗口
 */
public class Test02_OpenWindow {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        DataStreamSource<Event> stream = env.fromElements(
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

        // 给数据源添加水位线
        SingleOutputStreamOperator<Event> streamWithWM = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5)
                )
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // 对 带有水位线的数据源开窗
        WindowedStream<Event, String, TimeWindow> window = streamWithWM
                .keyBy(data -> data.user)
 //                     .countWindow(100)                                                           // 计数窗口
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))                       // 滚动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.milliseconds(10),Time.milliseconds(3)))   // 滑动事件时间窗口
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)));                    //事件事件会话窗口

        // 环境执行
        env.execute();
    }
}
