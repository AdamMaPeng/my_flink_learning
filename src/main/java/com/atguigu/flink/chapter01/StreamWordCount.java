package com.atguigu.flink.chapter01;

/**
 * @author Adam-Ma
 * @date 2022/5/3 10:42
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.utils.ConfigUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  使用 NC 客户端模拟真实的流数据处理过程
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1、准备 流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、读取 socket 中的数据
        DataStreamSource<String> lineDS =
                env.socketTextStream(ConfigUtils.getConfig("hostName"), Integer.parseInt(ConfigUtils.getConfig("port")));

        // 3、将 每行数据进行扁平化处理
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4、将处理好的 wordToOne 进行分组
        KeyedStream<Tuple2<String, Long>, String> wordToOneGroup = wordToOne.keyBy((data -> data.f0));

        // 5、 将 分好组的数据进行聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordToOneGroup.sum(1);

        // 6、将结果进行打印
        result.print();

        // 7、挂起 执行当前的程序
        env.execute();
    }
}
