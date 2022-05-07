package com.atguigu.flink.chapter05.sink;

/**
 * @author Adam-Ma
 * @date 2022/5/6 14:58
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 *  StreamingFileSink ：
 *      1、将数据写入到文件中
 *      2、通过 builder 构建器方式创建
 *      3、两种常见方式进行创建
 *          RowFormatBuilder
 *          BulkFormatBuilder
 *      4、解决了 wirteAsCsv 、writeAsText 的
 *          1）故障恢复状态一致性无法保障的问题
 *          2）多个任务不能同时写入到一个文件中，并行度为1
 *        的问题
 */
public class SinkToFile1 {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<Event> eventDS = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        // 获取 StreamingFileSink
        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(
                new Path("input/fileSink"),
                new  SimpleStringEncoder("UTF-8")
        )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                        .withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build()
                )
                .build();

        // 将数据写入到文件
        eventDS.map(data -> data.toString()).addSink(fileSink);
        //执行流
        env.execute();
    }
}
