package com.atguigu.flink.chapter01;

/**
 * @author Adam-Ma
 * @date 2022/5/3 22:28
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义 Source
 */
public class CustomSourceTest {
    public static void main(String[] args) throws Exception {
//       1、准备 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取 自定义数据源 : 实现 SourceFunction ，无法设置并行度
//        DataStreamSource<Student> customDS = env.addSource(new MySource());

        // 获取自定义数据源 ： 实现 ParallelSourceFunction , 可以设置并行度
        DataStreamSource<Student> customDS = env.addSource(new MyParallelSource()).setParallelism(2);

        // 自定义数据源打印
        customDS.print("自定义数据源");

        // 环境执行
        env.execute();
    }
}

/**
 * 自定义Source implements SourceFunction
 *      实现 run（） 方法
 *           cancel() 方法
 *          定义标志位
 *
  */
class MySource implements SourceFunction<Student>{
    // 定义一个标志位
    public boolean runningFlag = true;
    // 定义随机对象
    Random random = new Random();

    @Override
    public void run(SourceContext sc) throws Exception {
        int[] ids = {1001, 2001, 1023, 2301,3451};
        String[] names = {"Adam", "Leo", "Tom", "Jack"};
        int[] ages = {23,18, 34, 21, 45};

        while (runningFlag) {
            sc.collect(new Student(ids[random.nextInt(ids.length)],names[random.nextInt(names.length)], ages[random.nextInt(ages.length)]));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        runningFlag = false;
    }
}

/**
 *  自定义 ParallelSource ，implements ParallelSourceFunction
 */
class MyParallelSource implements ParallelSourceFunction<Student>{
    // 定义标志位
    public boolean runningFlag = true;
    // 定义 Randm 对象
    Random random = new Random();

    @Override
    public void run(SourceContext sc) throws Exception {
        while (runningFlag) {
            int[] ids = {1001, 2001, 1023, 2301,3451};
            String[] names = {"Adam", "Leo", "Tom", "Jack"};
            int[] ages = {23,18, 34, 21, 45};

            while (runningFlag) {
                sc.collect(new Student(ids[random.nextInt(ids.length)],names[random.nextInt(names.length)], ages[random.nextInt(ages.length)]));
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        runningFlag = false;
    }
}