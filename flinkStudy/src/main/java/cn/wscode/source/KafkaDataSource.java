package cn.wscode.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author wangSheng
 * @title: KafkaDataSource
 * @projectName DS
 * @description: TODO 从kafka的topic订阅数据
 * @date 2022/3/29 11:00
 */
public class KafkaDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 构建kafka的连接器
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaClient = env.addSource(new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), properties));
        kafkaClient.print();
        env.execute();


    }
}
