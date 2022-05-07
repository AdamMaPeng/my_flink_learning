package com.atguigu.flink.chapter05.sink;

/**
 * @author Adam-Ma
 * @date 2022/5/6 11:23
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import javafx.beans.property.SimpleIntegerProperty;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.planner.expressions.In;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 *   将数据写入到文件中： StreamingFileSink
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置总体并行度
        env.setParallelism(1);

        // 获取输入源
        DataStreamSource<String> DS = env.fromElements("1", "32", "43", "56", "65");

        // 创建 StreamingFileSink 的对象
        StreamingFileSink fileSink = StreamingFileSink.forRowFormat(new Path("input/fileSink"), new SimpleStringEncoder("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                                .withMaxPartSize(1024 * 1034 * 1024)
                                .build()
                ).build();

        // 将数据写入文件中
        DataStreamSink<String> integerDataStreamSink = DS.addSink(fileSink);

        // 环境执行
        env.execute();
    }
}








































































