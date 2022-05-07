package com.atguigu.flink.chapter05.sink;

/**
 * @author Adam-Ma
 * @date 2022/5/6 20:35
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import com.atguigu.flink.chapter05.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 *  将数据写入到 ES 中
 *
 */
public class SinkToElasticSearch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//
//        httpHosts.add(new HttpHost("hadoop102", 9200, "http"));
//    // 创建一个 ElasticsearchSinkFunction
//        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new
//                ElasticsearchSinkFunction<Event>() {
//                    @Override
//                    public void process(Event element, RuntimeContext ctx, RequestIndexer
//                            indexer) {
//                        HashMap<String, String> data = new HashMap<>();
//                        data.put(element.user, element.url);
//                        IndexRequest request = Requests.indexRequest()
//                                .index("clicks")
//                                .type("type") // Es 6 必须定义 type
//                                .source(data);
//                        indexer.add(request);
//                    }
//                };
//        stream.addSink(new ElasticsearchSink.Builder<Event>(httpHosts,
//                elasticsearchSinkFunction).build());

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));

        // 创建 ElasticSearchSinkFunction
        ElasticsearchSinkFunction elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            /**
             *  处理需要存储到 ElasticSearch 中的数据
             * @param event
             * @param runtimeContext
             * @param indexer
             */
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer indexer) {
                HashMap<String, String> data = new HashMap<>();
                data.put(event.user, event.url);
                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type") // Es 6 必须定义 type
                        .source(data);
                indexer.add(request);
            }
        };

        // 将数据写入到 ES 中
        stream.addSink(new ElasticsearchSink
                .Builder(
                  httpHosts,
                  elasticsearchSinkFunction
        ).build());

        env.execute();
    }
}
