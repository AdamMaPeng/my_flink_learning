package com.atguigu.flink.chapter05;

/**
 * @author Adam-Ma
 * @date 2022/5/4 15:14
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  Map 算子 测试：
 */
public class TransfromMapTest {
    public static void main(String[] args) throws Exception {
        // 1 、准备 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据源 ； fromElement()
        DataStreamSource<Student> studentDataStreamSource = env.fromElements(
                new Student(1001, "Adam", 25),
                new Student(1002, "Tom", 19),
                new Student(1003, "Jack", 23),
                new Student(1004, "Leo", 26)
        );

        // map : 自定义 类继承 MapFunction
//        studentDataStreamSource.map(new MyMapFunction()).print("M_1");

        // map : 匿名对象方式重写 map 方法
        /*
        studentDataStreamSource.map(new MapFunction<Student, Object>() {
            @Override
            public Object map(Student value) throws Exception {
                return value.id + "_" + value.name + "_" +  value.age;
            }
        }).print("M_2");
        */

        // map : lambda 表达式的方式
        studentDataStreamSource.map((Student stu) -> {
           return stu.id + " ^ " + stu.name + " ^ " + stu.age;
        }).print("M_3");

        // 提交执行
        env.execute();
    }

    /**
     *  自定义 类 实现 MapFunction ，重写 map 方法
     */
    public static class MyMapFunction implements MapFunction<Student, String>{
        @Override
        public String map(Student value) throws Exception {
            return value.id + " - " + value.name + " - " +  value.age;
        }
    }
}
