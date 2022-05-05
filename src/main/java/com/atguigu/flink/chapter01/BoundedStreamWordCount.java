package com.atguigu.flink.chapter01;

/**
 * @author Adam-Ma
 * @date 2022/5/3 10:14
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  Flink 有界流的方式
 *      执行 word Count 任务
 *
 *      对于批数据，进行处理的时候，使用的是 DataSet API，其执行环境为 ExecutionEnviroment
 *      对于流数据，进行处理的时候，使用的是 DataStream API，其执行环境为 StreamExecutionEnviroment
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1、准备 流处理环境：
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、获取数据源
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        // 3、对每行数据进行扁平化处理
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将每行字符串进行切分
            String[] words = line.split(" ");
            // 遍历 words
            for (String word : words) {
                // 使用 收集器收集处理过的数据
                out.collect(Tuple2.of(word, 1L));
            }
        })  // 对 Flink 进行处理过程中，传入 lambda 表达式，会进行泛型擦除，所以添加如下代码
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4、将（word，1）进行分组,对于流处理方式，没有groupBy ，是通过 keyBy进行分组的
        KeyedStream<Tuple2<String, Long>, String> wordToOneGroup = wordToOne.keyBy(data -> data.f0);

        // 5、针对分好组的数据进行聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordToOneGroup.sum(1);

        // 6、将结果进行打印
        result.print();

        // 7、由于是流数据的处理，需要将当前进程挂起，等待数据持续输入
        env.execute();
    }
}
