package com.atguigu.flink.chapter06;

/**
 * @author Adam-Ma
 * @date 2022/5/9 14:19
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.UrlView;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 *  测试 Flink 保证处理延迟数据的三重保障：
 *      水位线            ： Watermarks
 *      允许最大延迟      ： allowedLateness
 *      侧输出流          ： sideOutputLateData
 *
 *  需求：
 *      统计各个 url 被点击的次数
 */
public class Test09_LatenessSideOutput {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        //获取数据源
        SingleOutputStreamOperator<Event> streamWithWM = env.socketTextStream("hadoop102", 9999)
                .map(lineStr -> {
                    String[] fields = lineStr.split(" ");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(
                        // 水位线设置可以获取到延迟 2 秒中的数据
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        streamWithWM.print("Input :  ");

        // 定义侧输出流 需要的 输出标志
        OutputTag<Event> eventOutputTag = new OutputTag<Event>("late"){};


        // 将数据进行 keyBy 分组后，放置在 对应的窗口中
        SingleOutputStreamOperator<UrlView> resultStream = streamWithWM
                .keyBy(event -> event.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 设置窗口最大延迟时间为 60 秒，超过 水位线 + 最大延迟时间的 数据将存放到 侧输出流中
                .allowedLateness(Time.minutes(1))
                // 将 两次都延迟的数据放置到 侧输出流中
                .sideOutputLateData(eventOutputTag)
                .aggregate(new UrlAggregateFunction(), new UrlProcessWindowFunction());

        // 窗口 关闭后的流 进行打印
        resultStream.print("Result : ");

        // 打印侧输出流
        DataStream<Event> sideOutput = resultStream.getSideOutput(eventOutputTag);
        sideOutput.print("Late : ");

        // 执行环境
        env.execute();
    }

    // AggregateFunction 的实现类
    public static class UrlAggregateFunction implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // ProcessWindowFunction的实现类
    public static class UrlProcessWindowFunction extends ProcessWindowFunction<Long, UrlView, String, TimeWindow> {

        @Override
        public void process(String urlName, Context context, Iterable<Long> elements, Collector<UrlView> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long visitCount = elements.iterator().next();

            out.collect(new UrlView(urlName, visitCount, start, end));
        }
    }
}
