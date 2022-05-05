package com.atguigu.flink.chapter01;

/**
 * @author Adam-Ma
 * @date 2022/5/1 21:21
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 *  第一个 flink 程序
 *
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //  1、获取 Flink 运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、获取数据流
        env
                .socketTextStream("localhost", 9999)
                .setParallelism(1)
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                                String[] words = line.split(" ");
                                for (String word : words) {
                                    collector.collect(Tuple2.of(word , 1));
                                }
                            }
                        }
                )
                .setParallelism(1)
                .keyBy(
                    new KeySelector<Tuple2<String, Integer>, String>() {
                        @Override
                        public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                            return tuple2.f0;
                        }
                    }
                )
                .reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple2, Tuple2<String, Integer> t1) throws Exception {
                                return Tuple2.of(tuple2.f0, tuple2.f1 + t1.f1);
                            }
                        }
                )
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
