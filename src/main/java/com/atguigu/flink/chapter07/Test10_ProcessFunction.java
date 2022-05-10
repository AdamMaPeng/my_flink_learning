package com.atguigu.flink.chapter07;

/**
 * @author Adam-Ma
 * @date 2022/5/9 18:51
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 *  ProcessFunction 测试
 *      context 中提供的
 *          timeService
 *          timestamp
 *          processElement
 *
 */
public class Test10_ProcessFunction {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源
        SingleOutputStreamOperator<Event> streamWithWM = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // 将数据源按照 url 进行划分
        KeyedStream<Event, String> eventStringKeyedStream = streamWithWM.keyBy(event -> event.url);

        // 调用 process 方法
        eventStringKeyedStream.process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                // 获取上下文信息
                long currentWatermark = ctx.timerService().currentWatermark();
                long currentProcessingTime = ctx.timerService().currentProcessingTime();

                out.collect( value + "处理时间：" + new Timestamp(currentProcessingTime));

                // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(10 * 1000L);
            }

            // 开启定时器，在定时器中完成相应的功能
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);

                out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
            }
        })
                .print();

        // 环境执行
        env.execute();
    }

}
