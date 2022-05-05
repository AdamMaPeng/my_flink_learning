package com.atguigu.flink.chapter05;

/**
 * @author Adam-Ma
 * @date 2022/5/4 21:30
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  RichFunction :
 *      1、是一个抽象类
 *      2、RichMapFunction 等一般的非富函数的函数 extends AbstractRichFunction implement MapFunction
 *      2、有生命周期的概念，如 open、close 方法，每个任务只执行一次，而不会像之前的function ，一条数据来了执行一次
 *      3、提供了获取上下文的方法
 *      4、提供了设置、获取状态（这就更加体现了 Flink是有状态的流计算框架，之前的实例中，
 *         只有 reduce 会保留状态，而继承了 riceFunction 的其他function,
 *         如mymapFunction ，则可以保留状态，从而也可以实现 reduce 的功能）
 *      5、由于RichFunction 不是 SAM 的类， 所以继承了该类的类不能使用 lamdba 表达式
 */
public class MyRichMapFunctionTest {
    public static void main(String[] args) throws Exception {
        //  流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  获取数据源
        DataStreamSource<Student> studentDS = env.fromElements(
                new Student(1001, "Adam", 25),
                new Student(1001, "Adam_1", 27),
                new Student(1002, "Tom", 19),
                new Student(1001, "Adam_2", 45),
                new Student(1002, "Tom_1", 33),
                new Student(1003, "Jack", 23),
                new Student(1004, "Leo", 26)
        );

        // 调用 继承了 富函数的 map 方法
        studentDS.map(new MyRichMapFunction()).print();

        // 环境执行
        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<Student, Integer>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open 生命周期被调用" + getRuntimeContext().getTaskName() + " 任务被调用");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close 生命周期被调用" + getRuntimeContext().getTaskName() + " 任务结束");
            super.close();
        }

        @Override
        public Integer map(Student value) throws Exception {
            return value.id;
        }
    }
}
