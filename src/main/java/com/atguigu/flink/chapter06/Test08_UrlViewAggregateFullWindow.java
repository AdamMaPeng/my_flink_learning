package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/9 10:46
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.UrlView;
import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *  需求 :
 *      使用 增量聚合函数 AggregateFunction + 全窗口 ProcessWindowFunction 完成url 访问次数的统计
 */
public class Test08_UrlViewAggregateFullWindow {
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
        streamWithWM.print("input : ");

        streamWithWM
                .keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                .aggregate(new UrlAggregateFunction(),new UrlProcessWindowFunction())
                .print("result : ");
        // 环境执行
        env.execute();
    }

    // AggregateFunction 的实现类
    public static class UrlAggregateFunction implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
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

    // ProcessWindowFunction的实现类
    public static class UrlProcessWindowFunction extends ProcessWindowFunction<Long, UrlView, String, TimeWindow> {

        @Override
        public void process(String urlName, Context context, Iterable<Long> elements, Collector<UrlView> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long visitCount = elements.iterator().next();

            out.collect(new UrlView(urlName, visitCount, start, end));
        }
    }

}
