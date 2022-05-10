package com.atguigu.flink.chapter08;

/**
 * @author Adam-Ma
 * @date 2022/5/9 22:24
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
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.time.Duration;
import java.util.stream.Stream;

/**
 *     对于两个流中的数据类型不同的情况，如果想要进行了流的合并， 需要
 *     使用 Connect 进行连接，
 *      连接完，可以通过 map / flatmap / process 将多流中不同数据类型的数据转换成同一种数据类型
 */
public class Test02_ConnectStream {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 数据源1 ： socketTextStream
        SingleOutputStreamOperator<String> stream1 = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<String>() {
                                            @Override
                                            public long extractTimestamp(String element, long recordTimestamp) {
                                                return recordTimestamp;
                                            }
                                        }
                                )
                );

        // 数据源2 ： ClickSource
        SingleOutputStreamOperator<Event> stream2 = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );

        // 两个流 连接 ：Connect
        stream1.connect(stream2).map(new CoMapFunction<String, Event, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "Connect " + value;
            }

            @Override
            public String map2(Event value) throws Exception {
                return "Connect " + value.toString();
            }
        }).print("Result : ");


        // 环境执行
        env.execute();
    }
}
