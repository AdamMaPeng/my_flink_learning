package com.atguigu.flink.chapter05.PhysicalPartitioning;

/**
 * @author Adam-Ma
 * @date 2022/5/5 11:48
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.POJO.Student;
import org.apache.calcite.rel.core.Calc;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 *   物理分区： 将数据真实的发送到TaskMananger 中的不同的 任务所在的 slot 中
 */
public class PartitioningTest {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取数据源
        DataStreamSource<Student> studentDS = env.fromElements(
                new Student(1001, "Adam", 25),
                new Student(1001, "Adam_1", 27),
                new Student(1002, "Tom", 19),
                new Student(1002, "Tom_1", 33),
                new Student(1003, "Jack", 23),
                new Student(1001, "Adam_2", 45),
                new Student(1004, "Leo", 26),
                new Student(1005, "Jeo", 55)
        );

        // 测试并行度
        // rebalance() : 默认并行度, 不显示写入就是rebalance ，前一个任务会轮询的将数据发送给下级每个任务
//        studentDS.print().setParallelism(4);
//        studentDS.rebalance().print().setParallelism(4);

        // shuffle() : 随机，均匀的分配到下级任务
//        studentDS.shuffle().print().setParallelism(4);

        // rescale() :  将数据轮询发送到下游并行任务中
        /**
         *   0号分区的数 ： 2     4       6       8
         *   1号分区的数 :  1     3       5       7
         *
         *   在下游进行分发： 2/4/6/8 发送到 1，2 并行任务中
         *
         */
//        DataStreamSource<Integer> intDS = env.addSource(new MyParallelSource1()).setParallelism(2);
//        intDS.rescale().print().setParallelism(4);

        // broadcast : 广播发送，每个数据都会向下游所有的并行任务中进行发送
//        studentDS.broadcast().print().setParallelism(2);

        // global  :  全局发送，会将数据都发送到一个分区中 , 即使设置了并行度也不管用
//        studentDS.global().print().setParallelism(2);

        // PartitionCustom : 自定义分区
        DataStreamSource<Integer> intDS = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8,9,10,190);
        intDS.partitionCustom(
                new Partitioner<Integer>() {
                    /**
                     *  小于 4 的数据进入一个分区，大于 4 的进入一个分区
                     */
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key <= 4 ? 0 : 1;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }
        )
                .print()
                .setParallelism(2);

        // 流执行
        env.execute();
    }

    public static class MyParallelSource1 extends RichParallelSourceFunction<Integer> {
        public boolean flag = true;

        @Override
        public void run(SourceContext<Integer> sc) throws Exception {
            for (int i = 1; i <= 8; i++) {
                // 将 奇偶数分别发送到 0号 和 1 号并行分区中
                if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()){
                    sc.collect(i);
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
