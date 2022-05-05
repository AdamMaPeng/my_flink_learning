package com.atguigu.flink.chapter05;

/**
 * @author Adam-Ma
 * @date 2022/5/4 19:21
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  测试 KeyBy  聚合算子
 *      keyBy原理： 将数据的key % 分区数 ，求出数据的逻辑分区
 *      简单聚合：
 *          sum()
 *          min()
 *          minBy()
 *          max()
 *          maxBy()
 */
public class AggrateKeyByTest {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        DataStreamSource<Student> studentDS = env.fromElements(
                new Student(1001, "Adam", 25),
                new Student(1001, "Adam_1", 27),
                new Student(1002, "Tom", 19),
                new Student(1001, "Adam_2", 45),
                new Student(1002, "Tom_1", 33),
                new Student(1003, "Jack", 23),
                new Student(1004, "Leo", 26)
        );

        studentDS.keyBy(student -> student.id)
                // 简单聚合
                // max() ：打印 最大值变化，其他字段不变化
//                .max("age").print();
                // maxBy() : 打印最大值那一行
//                .maxBy("age").print();
                // sum() : 求每个 id 年龄的总和
                .sum("age").print();

        // 执行环境
        env.execute();
    }
}
