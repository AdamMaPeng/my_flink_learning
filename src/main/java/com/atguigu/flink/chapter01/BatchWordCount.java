package com.atguigu.flink.chapter01;

/**
 * @author Adam-Ma
 * @date 2022/5/2 22:41
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 *  使用 DataSet API 完成 Flink 的批处理
 *      实现 WordCount 的功能
 *
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1、准备批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 2、读取输入数据
        DataSource<String> lineDataSource = env.readTextFile("input/word.txt");

        // 3、将一行数据进行扁平化处理
        FlatMapOperator<String, Tuple2<String, Long>> wordToOne = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");

            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        // 4、将 当个单词进行分组,分组规则为 Tuple 中的 String
        UnsortedGrouping<Tuple2<String, Long>> wordToOneGroup = wordToOne.groupBy(0);

        // 5、将分好组的数据，进行聚合统计
        AggregateOperator<Tuple2<String, Long>> result = wordToOneGroup.sum(1);

        // 6、打印最终聚合后的结果
        result.print();
    }
}
