package com.atguigu.flink.chapter09;

/**
 * @author Adam-Ma
 * @date 2022/5/12 10:28
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 *
 * 状态 ： 当执行某项并行子任务时，除了当前过来的数之外，还要依托一些其他的数据，这个其他的数据就称之为状态，
 *        辅助任务计算的数据
 *
 *   算子分类（从状态的角度）：
 *          有状态算子：运行时需要依托其他的数据的算子
 *              window      ： 来到窗口中的数据都是要保存下来的
 *              aggregate   ： 需要将数据进行 聚合累积，acc 状态
 *              process     : 所有的 process function ，
 *              富函数      ：
 *
 *          无状态算子： map / flatmap / filter
 *              执行算子任务的时候，不会引用到其他的数据，只根据当前数据的数据直接转换输出结果
 *
 *    用法：
 *          只能通过 getRuntimeContext().getState（）进行使用，才能保证是在单个并行子任务中的使用
 *
 *
 *  状态的分类
 *      托管状态 （Manager State） ： 通过 key 划分 state ，state 故障恢复， state 重组调整都是通过 Flink 自己完成的
 *          概念：
 *              1、是由 Flink 的运行时 (Runtime) 来托管的
 *              2、配置了容错机制时，状态会自动持久化保存，发生故障的时候，会自动恢复
 *              3、发生应用横向扩展时，状态会自动重组调整到各个子任务上
 *
 *          根据状态内容，Flink 提供了：（内部支持各种数据类型）
 *              值状态      （ValueState）
 *              列表状态    （ListState）
 *              映射状态    （MapState）
 *              聚合状态    （AggregateState）
 *
 *           常见的托管状态：
 *              聚合中的内置状态
 *              窗口中的内置状态
 *              在富函数类（RichFunction）中，通过上下文 自定义的状态
 *          分类：
 *              算子状态（ operator state）
 *                  状态的作用范围为当前算子的任务实例，也就是只对当前并行子任务实例有效
 *                  并行子任务中所有的数据都可以访问到当前状态
 *
 *                  使用： 使用时需要 实现 CheckPointFuntion 接口
 *
 *                  本质：相当于一个本地变量，所有算子并行子任务中的数据都可以进行访问
 *
 *              按键分区状态（ Keyed State） ：任务按照 key 来维护和访问的状态
 *                  单个并行子任务中，会根据key 的不同，维护该 key 相关的状态
 *                  1、根据输入流中 key 来进行维护和访问
 *                  2、只能定义在 按键分区流（KeyStream） 中
 *                  3、只有 keyBy 以后才可以使用
 *                  4、也可以通过 富函数类（RichFunction）自定义 Keyed State
 *                  5、提供了富函数类接口的算子，也可以使用 Keyec State
 *
 *                  常见的 Keyed State ：
 *                      1、keyBy 之后 的 Aggregate ， Reduce 等的聚合算子
 *                      2、窗口算子： 不同的key 维护起来
 *                      3、RichFunction 富函数中自定义的状态，也是 Keyed State
 *
 *                  6、Keyed State 所支持的结构类型
 *                      ValueState<T>       值状态 ： 状态中只保存一个值（value）,T 为泛型，对应Flink 支持的所有数据类型
 *                              T value() : 获取当前值的状态
 *                              update（）： 覆写当前值的状态
 *                      ListState           列表状态
 *                      MapState            映射状态
 *                      ReducingState       归约状态
 *                      AggregatingState    聚合状态
 *
 *                  7、怎么创建 Keyed State ：
 *                      在 所有的继承了 RichFunction 的子类的类中，通过open 生命周期，在 open 中
 *                      调用 getRuntimeContext().getXXXState(new XXXStateDescriptor()) 创建即可。
 *                      创建的对象需要声明在 open（）方法之外，供其他的方法也可以使用该状态。
 *
 *      原始状态（Raw state）： 所有都需要自定义。
 *                            底层将状态以 byte[] 保存，所有的状态管理都需要开发者自己完成，
 */

/**
 *  通过代码可以验证：
 *      Keyed State 是根据 key 进行维护的，
 *          对于keyed state 所支持的数据结构来说，只会维护自己 key 所对应的 state
 *          而如果添加了本地变量，本地变量的变化，则不会随着 key 的不同而有变化。是来一条数据，变化一次
 */
public class Test01_State {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 获取数据源你
        SingleOutputStreamOperator<Integer> streamWithWM = env.fromElements(1, 2, 3, 4, 5)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Integer>() {
                                    @Override
                                    public long extractTimestamp(Integer element, long recordTimestamp) {
                                        return recordTimestamp;
                                    }
                                })
                );

        // 先进行 keyBy ，然后map
        streamWithWM
                .keyBy(data -> true)
                .map(new MyStateMapFunction())
                .print("StateMapFunction : ");

        // 环境执行
        env.execute();
    }

    public static class MyStateMapFunction extends RichMapFunction<Integer, Integer> {
        // 定义 值状态 ，没来一个数据调用一次，保存当前的状态
        private ValueState valueState;
        // 定义 列表状态，将数据都保存到 List 中
        private ListState listState;
        // 定义 映射状态，将数据以 k-v 形式保存到 Map 中
        private MapState mapState;
        // 定义 规约状态 ，对数据进行规约计算
        private ReducingState reducingState;
        // 定义 聚合状态， 对数据进行 一般化的操作
        private AggregatingState aggregatingState;

        // 需要调用 运行时上下文的 getState 方法，但是对于 map算子，每条数据都会调用一次map ，所以需要在生命周期中调用 getState，而不是直接调用
            @Override
            public void open(Configuration parameters) throws Exception {
            // 在 运行时上下文中，获取到 ValueState 的对象
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-valueState", Integer.class));

            // 获取 ListState 的对象
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("my-listState", Integer.class));

            // 获取 MapState 对象
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("my-mapState", Integer.class, String.class));

            // 获取 ReducingState 对象
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("my-reduceState", new ReduceFunction<Integer>() {
                // 聚合逻辑： 求输入数据的和
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    return value1 + value2;
                }
            }, Integer.class));

            // 获取 AggregateState 对象
                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Long, String>("my-aggregateState", new AggregateFunction<Integer, Long, String>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Integer value, Long accumulator) {
                        return accumulator + value;
                    }

                    @Override
                    public String getResult(Long accumulator) {
                        return "通过 Aggregate 算的结果为 " + accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },  Long.class));
        }

        @Override
        public Integer map(Integer value) throws Exception {
                 // 一开始获取ValueState 的数据 ： 最初 state 没有保存
            System.out.println("init    > valueState: " + valueState.value());
            valueState.update(value);
            System.out.println("result  > valueState: " + valueState.value());

            // 获取 ListState 的数据
            System.out.println("init    > listState: " + listState.get());
            listState.add(value);
            System.out.println("result  > ListState ：" + listState.get());

            // 获取 MapState 的状态
            System.out.println("init    > MapState ： " + "key: " + value + " - value: " + mapState.get(value));
            mapState.put(value, "mapState-"+value);
            System.out.println("result  > MapState ： " + "key: " + value + " - value: " + mapState.get(value));

            // 获取 ReducingState 的状态
            System.out.println("init   > ReducingState ：" + reducingState.get());
            reducingState.add(value);
            System.out.println("result > ReducingState ：" + reducingState.get());

            // 获取 AggregatingState
            System.out.println("init   > AggregatingState : " + aggregatingState.get());
            aggregatingState.add(value);
            System.out.println("result > AggregatingState : " + aggregatingState.get());
            System.out.println("----------------------------------------------------------------------------");
            return value;
        }
    }
}
