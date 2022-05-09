package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/8 21:24
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;

/**
 *  全窗口函数：
 *      WindowFunction
 *      ProcessWindowFunction
 *
 *  需求： 获取每个窗口中 UV 的信息
 */
public class Test06_FullWindowFunction {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        streamWithWM.print("Input : ");

        streamWithWM
                .keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new MyProcessFunction())
                .print("Result : ");

        // 环境执行
        env.execute();
    }

    public static class MyProcessFunction extends ProcessWindowFunction<Event, String, Boolean, TimeWindow>{
        // processWindowFunction 中需要实现的具体方法
        /**
         *
         * @param aBoolean      key 的类型
         * @param context       通过ProcessWindowFunction 可以获取到的上下文对象
         * @param elements      全部窗口中的数据
         * @param out           最终的输出类型
         * @throws Exception
         */
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            // 获取当前上下文的信息
            long start = context.window().getStart();
            long end = context.window().getEnd();

            // 通过 Iterable 迭代器对象，获取到当前集合中独立的访客信息
            HashSet<Event> uv = new HashSet<>();
            Iterator<Event> iterator = elements.iterator();
            while (iterator.hasNext()) {
                uv.add(iterator.next());
            }

            // 将结果进行输出
            out.collect("窗口 1 ：开始时间" + start + "， 结束时间: " + end + ", 共有访客： " +uv.size());
        }
    }
}
