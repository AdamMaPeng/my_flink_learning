package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/8 15:40
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 *  窗口函数：
 *      开启窗口，
 *      测试窗口函数：
 *        流式处理
 *          增量聚会函数：
 *              聚合函数： AggregateFunction
 *              规约函数： ReduceFunction
 *
 *        批式处理：
 *            全窗口函数：
 *
 */
public class Test03_WindowOperation {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStreamSource<Event> stream = env.fromElements(
//                new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./prod?id=100", 3000L),
//                new Event("Alice", "./prod?id=200", 3500L),
//                new Event("Bob", "./prod?id=2", 2500L),
//                new Event("Alice", "./prod?id=300", 3600L),
//                new Event("Bob", "./home", 3000L),
//                new Event("Bob", "./prod?id=1", 2300L),
//                new Event("Bob", "./prod?id=3", 3300L)
//        );

        // 获取数据源
        DataStreamSource<Event> stream = env.addSource(
                new ClickSource()
        );

        // 针对数据源 添加 水位线
        SingleOutputStreamOperator<Event> streamWithWM = stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // 将 带有水位线的数据转换格式后， 进行分区，按照用户名进行分区
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = streamWithWM
                .map(data -> Tuple2.of(data.user, 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .keyBy(tuple2 -> tuple2.f0);

        // 将分好区的数据 放入窗口内,对窗口中的数据进行聚合
        tuple2StringKeyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        (tuple1, tuple2) -> Tuple2.of(tuple1.f0, tuple1.f1 + tuple2.f1)
                )
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .print();

        // 针对窗口中的数据进行聚合，求出

        // 环境执行
        env.execute();
    }
}
