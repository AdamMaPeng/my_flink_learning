package com.atguigu.flink.chapter05.sink;

/**
 * @author Adam-Ma
 * @date 2022/5/6 15:51
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import akka.actor.Props;
import com.sun.media.sound.PortMixerProvider;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 *    1、将 文件中的数据读取出来，写入到 Kafka 的topic
 *    2、从 kafka 的 A topic 读取文件， 写入到 Kafka 的B topic
 *
 */
public class SinkDataToKafka {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 env 的并行度
        env.setParallelism(1);

        // 从文件中读取数据写入到 Kafka 中
        String filePath = "input/word.txt";
        String topicA = "flinkData";
//        FileDataToKafka(env, filePath, topicA);


        // 从 kafka 中消费数据，再将消费到的数据写入到 Kafka 中
        String topicB = "flinkToKafka";
        KafkaDataToKafka(env, topicA, topicB);
    }

    /**
     *  将从文件中读取到的数据 ，写入到 Kafka 的指定 topic 中
     */
    public static void FileDataToKafka(StreamExecutionEnvironment env, String filePath, String topic) throws Exception {
        // 1、从文件中读取数据
        DataStreamSource<String> lineDS = env.readTextFile(filePath);

        // 2、将 每行读取到的数据写入到 Kafka 的 topic 中
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");

        lineDS.addSink(new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                props
        ));

        env.execute();
    }

    /**
     *   将从 kafka 的 A topic 读取文件， 写入到 Kafka 的B topic
     */
    public static void KafkaDataToKafka(StreamExecutionEnvironment env, String topicA, String topicB) throws Exception {
        // 获取 Kafka topicA 中的数据
        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop102:9092");
        props.put("group.id","consumer-group");
        props.put("auto.offset.commit","latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> lineDS = env.addSource(new FlinkKafkaConsumer<String>(
                topicA,
                new SimpleStringSchema(),
                props
        ));

        //  将 从 Kafka topic A中的数据写入 到 topicB 中
        Properties props1 = new Properties();
        props1.put("bootstrap.servers","hadoop102:9092");

        lineDS.addSink(
                new FlinkKafkaProducer<String>(
                        topicB,
                        new SimpleStringSchema(),
                        props1
                )
        );

        env.execute();
    }
}
