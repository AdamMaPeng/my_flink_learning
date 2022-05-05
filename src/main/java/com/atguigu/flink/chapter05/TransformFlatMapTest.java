package com.atguigu.flink.chapter05;

/**
 * @author Adam-Ma
 * @date 2022/5/4 16:09
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  完成 FlatMap 算子的测试
 */
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        DataStreamSource<Student> studentDS = env.fromElements(
                new Student(1001, "Adam", 25),
                new Student(1002, "Tom", 19),
                new Student(1003, "Jack", 23),
                new Student(1004, "Leo", 26)
        );

        // flatMap ： 匿名内部类
//        studentDS.flatMap(new FlatMapFunction() {
//            @Override
//            public void flatMap(Object value, Collector out) throws Exception {
//                out.collect(((Student)value).id);
//                out.collect(((Student)value).name);
//                out.collect(((Student)value).age);
//            }
//        }).print("M_1");

        // flatMap : 自定义类 实现 FilterFunction
        studentDS.flatMap(new MyFlatMapFunction()).print("M_2");

        // flatMap : lambda 表达式方式事项
        studentDS.flatMap((Student student, Collector<String> out) ->{
            out.collect(student.age + "");
            out.collect(student.name + "");
            out.collect(student.id + "");
        }).returns(new TypeHint<String>() {
            @Override
            public TypeInformation<String> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).print("M_2");

      // 执行
        env.execute();
    }

    /**
     *  自定义类实现 FlatMapFunction
     */
    public static class MyFlatMapFunction implements FlatMapFunction<Student, String>{
        @Override
        public void flatMap(Student value, Collector<String> out) throws Exception {
            out.collect(value.id + "");
            out.collect(value.name + "");
            out.collect(value.age + "");
        }

    }
}
