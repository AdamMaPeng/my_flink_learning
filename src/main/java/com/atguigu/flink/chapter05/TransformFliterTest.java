package com.atguigu.flink.chapter05;

/**
 * @author Adam-Ma
 * @date 2022/5/4 15:34
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  Fliter 算子 测试：
 *
 */
public class TransformFliterTest {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    /*
        // 流的数据源 : Kafka
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");
        properties.put("group.id", "consumer-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        DataStreamSource<String> flinkKafkaDS = env.addSource(new FlinkKafkaConsumer<String>("flinkData", new SimpleStringSchema(), properties));
    */

        // 流的数据源 ： fromElements
        DataStreamSource<String> stringDS = env.fromElements("I have a dream", "big data, I want to be a big data Enginer", "hello", "Adam");

        // filter : lambda 表达式方式实现
//        stringDS.filter(data -> data.length() > 10).print("M_1");

        // filter : 自定义类 实现 FilterFunction
        stringDS.filter(new MyFilterFunction()).print("M_2");

        // filter ： 匿名内部类
        stringDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.length() > 10;
            }
        }).print("M_3");

        // 执行
        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<String>{


        @Override
        public boolean filter(String value) throws Exception {
            return value.length() > 10;
        }
    }
}














