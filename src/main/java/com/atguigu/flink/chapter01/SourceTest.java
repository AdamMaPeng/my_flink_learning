package com.atguigu.flink.chapter01;

/**
 * @author Adam-Ma
 * @date 2022/5/3 21:48
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import com.atguigu.flink.utils.ConfigUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *  Flink 源算子：
 *      1、readTextFile ： 读取文本中的数据，将文本中的数据作为数据源
 *      2、socketTextStream: 从 socket 中读取数据
 *      3、fromCollection： 从集合中读取数据
 *      4、fromElement ： 直接从一个个数据中读取数据u
 *      5、addSource （new FlinkKafkaConsumer()）
 *
 *      5、自定义数据源 ： 创建类实现 SourceFunction （无法进行并行度设置）
 *      6、自定义数据源（可设置并行度）： 创建类实现 ParalleSourceFunction
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 1、准备 数据流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1) 读取文本中的数据： readTextFile : 可以指定本地文件路径，也可以指定 HDFS中的文件路径
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        // 2) 读取socket 端的数据： socketTextStream
        DataStreamSource<String> socketDS = env.socketTextStream(ConfigUtils.getConfig("hostName"), Integer.parseInt(ConfigUtils.getConfig("port")));

        // 3) 把集合作为数据源
        List<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);

        DataStreamSource<Integer> collectionDS = env.fromCollection(list);

        // 创建 POJO 类的 集合
        List<Student> stuList = new ArrayList<>();
        stuList.add(new Student(1001,"Adam",23));
        stuList.add(new Student(1002,"Tom",12));
        stuList.add(new Student(1003,"Leo",34));
        stuList.add(new Student(1004,"jACK",23));
        DataStreamSource<Student> stuCollectionDS = env.fromCollection(stuList);

        // 4) fromElement
        DataStreamSource<String> fromElementsDS = env.fromElements("hello", "World", "Hello", "Scala");
        
        // 5) addSource（FlinkKafkaConsumer）
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> flinkData = env.addSource(new FlinkKafkaConsumer<String>("flinkData", new SimpleStringSchema(), properties));

        // 打印数据源：
//        lineDS.print("Method1 : readTextFile");
//        socketDS.print("Method2 : socketTextStream");
//        collectionDS.print("Method3 : fromCollection");
//        stuCollectionDS.print("Method3 : fromCollection");
//        fromElementsDS.print("Method4 : fromElements");
        flinkData.print("Method5:addSource(FlinkKafkaConsumer)");

        // env执行环境
        env.execute();
    }
}
