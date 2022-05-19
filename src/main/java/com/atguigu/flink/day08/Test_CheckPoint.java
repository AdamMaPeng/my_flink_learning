package com.atguigu.flink.day08;

/**
 * @author Adam-Ma
 * @date 2022/5/16 10:47
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.ClickSource;
import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

/**
 *  测试Flink 中的检查点
 *      状态的检查点，通过 env 进行设置
 *          env.enableCheckpointing(time)             : 给定义一个时间，来设置周期型的保存状态，设置这样的检查点
 *          env.setStateBackend(new FsStateBackend()) : 设置检查点的存放位置
 *      Flink 中状态通过检查点进行保存的逻辑：
 *          1）作业管理器，向输入数据源的多个并行子任务注入检查点分界线
 *          2）检查点分界线路过哪个并行子任务，就保存哪个子任务的状态快照
 *          3）分流时，将检查点分界线广播发送出去
 *          4）合流时，检查点分界线对齐
 *          5）合流时，接收到所有的检查点分界线后，才会做快照
 *          6）合并成为一个，才会往下游sink进行发送，然后对 sink 做快照
 *          7）每一个并行子任务都做完快照后
 *          8）会向作业管理器发送通知
 *          9）作业管理器接收到所有并行子任务的通知后，确认本次检查点完成
 *          10）将上一次的检查点文件删除，保留本次的检查点文件
 *          11）同时向所有的并行子任务广播一条本次检查点快照保存完成的通知
 *
 */
public class Test_CheckPoint {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100L);

        // 设置检查点
        // 设置10 秒钟保存一次检查点
        // 默认从 flink 1.12 开始，只保存最近一次的检查点
        env.enableCheckpointing(10 * 1000L);
        // 设置检查点文件的保存路径
        env.setStateBackend(new FsStateBackend("file:///E:/WorkSpace/my_flink_learning/src/main/resources/ckpts"));

        // 获取数据源
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                     WatermarkStrategy.<Event>forMonotonousTimestamps()
                     .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                         @Override
                         public long extractTimestamp(Event element, long recordTimestamp) {
                             return element.timestamp;
                         }
                     })
                ).print();
        // 执行环境
        env.execute();
    }
}
