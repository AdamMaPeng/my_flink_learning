package com.atguigu.zuoTeacher.day08;

/**
 * @author Adam-Ma
 * @date 2022/5/16 10:44
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *  Flink 中设置路径，保存检查点
 */
public class Test01_CheckPoint {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100L);

        //获取数据源
//        env.fromElements(new )
        // 环境执行
        env.execute();
    }
}
