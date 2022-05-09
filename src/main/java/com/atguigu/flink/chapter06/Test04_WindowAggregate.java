package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/8 19:11
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 *  窗口中的
 *      增量聚合函数：
 *          规约函数：Reduce Function
 *          聚合函数：Aggregate Function
 *
 *      全窗口函数：
 *
 *   需求： 获取到当前Event 中每个 用户浏览网页的 平均时长
 *
 */
public class Test04_WindowAggregate {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 给每个数据添加 水位线
        SingleOutputStreamOperator<Event> streamWithWM = stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // 将数据 根据 key进行分组
        SingleOutputStreamOperator<Long> aggregateResult = streamWithWM
                .keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                    // 创建累加器：累加器的初始状态
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L, 0L);
                    }

                    // 将输入的数据添加到累加器上
                    @Override
                    public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1L);
                    }

                    // 获取最终的结果
                    @Override
                    public Long getResult(Tuple2<Long, Long> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    // 会话窗口的场景下可能会用到，当前场景下不使用
                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        aggregateResult.print();

        // 环境执行
        env.execute();
    }
}
