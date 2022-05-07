package com.atguigu.flink.chapter05.sink;

/**
 * @author Adam-Ma
 * @date 2022/5/6 19:14
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 *   将数据写入到 Redis 中
 */
public class SinkToRedis {
    public static void main(String[] args) throws Exception {
        // 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据源
        DataStreamSource<Event> eventDS = env.fromElements(
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        // 创建 一个 连接到 Redis 的配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .build();

        // 创建 RedisSink extends RichSinkFunction
        eventDS.addSink(
                new RedisSink<Event>(
                        config,
                        new MyRedisMapper()
                )
        );

        // 执行流
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event>{
        /**
         *   使用Redis ，需要考虑
         *   数据类型：
         *   key
         *   value
         *   写入API
         *   获取 API
         *   所以此处， 命令描述，就是需要 写入 API
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"Event");
        }

        /**
         * 从 key 中获取数据
         * @param event
         * @return
         */
        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        /**
         *  从 key 中获取 value
         * @param event
         * @return
         */
        @Override
        public String getValueFromData(Event event) {
            return event.url;
        }
    }
}
