package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/9 9:27
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 *  增量聚合函数 + 全窗口函数 同时使用：
 *      1、既满足数据来一条处理一条的实时效果
 *      2、又可以获取到每一批次窗口的上下文相关信息
 *
 *  使用方式： .aggregate(, new ProcessWindowFunction（）{})
 *
 *  需求： 使用 AggregateFunction + ProcessWindowFunction 实现 UV
 *        打印出窗口信息
 *
 */
public class Test07_AggregateFullWindow {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源
        SingleOutputStreamOperator<Event> streamWithWM = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })

                );
        streamWithWM.print("input ： ");

        // 对数据进行分区
        streamWithWM
                .keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction())
                .print("result");

        // 环境执行
        env.execute();
    }

    // 实现 MyAggregateFunction
    public static class MyAggregateFunction implements AggregateFunction<Event, Long, Long> {
        HashSet<String> set = new HashSet<>();

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            set.add(value.user);
            return (long)set.size();
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现 MyProcessWindowFunction
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            // 获取上下文对象
            long start = context.window().getStart();
            long end = context.window().getEnd();

            // 拼接返回
            out.collect("窗口的开始时间：" + new Timestamp(start) + ", 结束时间：" + new Timestamp(end) + ", 访客人数为: " + elements.iterator().next());
        }
    }
}
