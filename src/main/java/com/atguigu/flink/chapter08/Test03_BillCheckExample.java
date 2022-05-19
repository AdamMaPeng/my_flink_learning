package com.atguigu.flink.chapter08;

/**
 * @author Adam-Ma
 * @date 2022/5/10 10:36
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 *  需求：
 *      针对来自 不同平台下的多个订单进行对账，
 *          当出现相同的账单时，则证明帐平的，输出相应的信息，
 *          当帐不平时，输出提示信息
 *
    通过 connect  连接两条流，在 connect 连接后进行
 */
public class Test03_BillCheckExample {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源 ； app 端
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );

        // 获取数据源 ： third-party
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                        return element.f3;
                                    }
                                }
                        )
        );

        //  两条流进行合并，类似与 SQL 中的 join ，相同的 order_id 进行合并
        appStream.connect(
                thirdPartyStream
        ).keyBy(app -> app.f0, thirdParty -> thirdParty.f0)
                .process(new MyCheckBillProcessFunction())
                .print("CheckResult : ");
        // 环境执行
        env.execute();
    }

    public static class MyCheckBillProcessFunction extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态，用来保存已经到来的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        // 状态变量是随着 执行环境走的，所以需要通过当前的生命周期获取到当前的执行环境
        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING,Types.LONG))
            );

            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thridParty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        // app 端的数据处理逻辑
        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 在 app 端，如果 第三方 的值存在，那么证明 平账成功
            if (thirdPartyEventState.value() != null) {
                out.collect("对账成功： " + value + " - " + thirdPartyEventState.value());
                // 平账成功，则清除第三方状态
                thirdPartyEventState.clear();
            }else {
                // 如果 第三方的值没有到来，更新当前 app 状态，添加一个定时器进行等待
                appEventState.update(value);
                // 注册一个5秒后 的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5 * 1000L);
            }
        }

        // thirdParty 端的数据处理逻辑
        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 在 app 端，如果 第三方 的值存在，那么证明 平账成功
            if (appEventState.value() != null) {
                out.collect("对账成功： " + value + " - " + appEventState.value());
                // 平账成功，则清除 app 端 状态
                appEventState.clear();
            }else {
                // 如果 第三方的值没有到来，更新当前 app 状态，添加一个定时器进行等待
                thirdPartyEventState.update(value);
                // 注册一个5秒后 的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3 + 5 * 1000L);
            }
        }

        // 设置定时器
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 由于 在两条流的处理逻辑中，当平账后，valueState 被清理了，所以，如果没有平账，则valueState 的值存在
            if (appEventState.value() != null) {
                // 进行提示
                out.collect("平账失败： " + appEventState.value() + " - 第三方支付信息未到达");
            }
            if (thirdPartyEventState.value() != null) {
                out.collect("平账失败 ： " + thirdPartyEventState.value() + " - app端支付信息未到达");
            }

            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
