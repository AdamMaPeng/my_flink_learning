package com.atguigu.flink.chapter08;

/**
 * @author Adam-Ma
 * @date 2022/5/9 22:15
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 *  对于多个流中的数据类型相同时， 对多个流的合并采用 Union ，合并后的结果为 DataStream
 */
public class Test01_UnionStream {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源1  : hadoop102  nc
        SingleOutputStreamOperator<String> hadoop102Stream = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        return recordTimestamp;
                                    }
                                })
                );
        hadoop102Stream.print("hadopo102 : ");

        // 获取数据源1  : hadoop103  nc
        SingleOutputStreamOperator<String> hadoop103Stream = env.socketTextStream("hadoop103", 9999)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                                    @Override
                                    public long extractTimestamp(String element, long recordTimestamp) {
                                        return recordTimestamp;
                                    }
                                })
                );
        hadoop103Stream.print("hadopo103 : ");

        // 对两条流进行合并
        DataStream<String> unionStream = hadoop102Stream.union(hadoop103Stream);
        unionStream.print("Union : ");

        // 环境执行
        env.execute();
    }
}
