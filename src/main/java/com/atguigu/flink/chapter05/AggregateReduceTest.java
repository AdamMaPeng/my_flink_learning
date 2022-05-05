package com.atguigu.flink.chapter05;

/**
 * @author Adam-Ma
 * @date 2022/5/4 20:00
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  Reduce 聚合算子的 测试
 *      规约聚合
 *         两两进行规约，规约的结果作为下一次的规约其中之一，另一个则为最新的数据
 *      需求：
 *          求出访问最多的 id 以及次数
 */
public class AggregateReduceTest {
    public static void main(String[] args) throws Exception {
        // 流环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        env.fromElements(
                new Student(1001, "Adam", 25),
                new Student(1001, "Adam_1", 27),
                new Student(1002, "Tom", 19),
                new Student(1002, "Tom_1", 33),
                new Student(1003, "Jack", 23),
                new Student(1001, "Adam_2", 45),
                new Student(1004, "Leo", 26)
        )
                .map(Student -> Tuple2.of(Student.id, 1L))
                    .returns(new TypeHint<Tuple2<Integer, Long>>() {})
                .keyBy(data -> data.f0)
                .reduce((Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) -> (Tuple2.of(value1.f0, value1.f1 + value2.f1)))
                    .returns(new TypeHint<Tuple2<Integer, Long>>() {})
                .keyBy(data -> "key")
                .reduce((Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) -> (value1.f1 > value2.f1 ? value1 : value2))
                    .returns(new TypeHint<Tuple2<Integer, Long>>() {})
                .print();

        // 执行环境
        env.execute();
    }
}
