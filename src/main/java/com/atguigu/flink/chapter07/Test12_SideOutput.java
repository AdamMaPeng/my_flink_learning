package com.atguigu.flink.chapter07;

/**
 * @author Adam-Ma
 * @date 2022/5/9 21:36
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 *  侧输出流 ：
 *      1、可以通过process 算子实现
 *        .process(
 *          xxxx.output
 *        )
 *
 *      2、开窗后，将延迟的数据写入到侧输出流中
 *          .window()
 *          .sideOutputLateData() :
 */
public class Test12_SideOutput {
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

        // 定义分流的输出标志
        OutputTag<Event> Mary = new OutputTag<Event>("Mary"){};
        OutputTag<Event> Bob = new OutputTag<Event>("Bob"){};

        SingleOutputStreamOperator<Event> processedStream = streamWithWM.process(
                new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                        if (value.user == "Mary") {
                            ctx.output(Mary, value);
                        } else if (value.user == "Bob") {
                            ctx.output(Bob, value);
                        } else {
                            out.collect(value);

                        }
                    }
                }
        );

        // 打印侧输出流
        processedStream.getSideOutput(Mary).print("Mary : ");
        processedStream.getSideOutput(Bob).print("Bob : ");
        processedStream.print("Other : ");




        // 环境执行
        env.execute();
    }
}
