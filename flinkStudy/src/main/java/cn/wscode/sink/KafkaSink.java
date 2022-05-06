package cn.wscode.sink;

import cn.wscode.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author wangSheng
 * @title: KafkaSink
 * @projectName DS
 * @description: TODO
 * @date 2022/3/30 14:52
 */
public class KafkaSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 构建kafka的连接器
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        DataStreamSource<String> kafkaClient = env.addSource(new FlinkKafkaConsumer<String>("topic", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> stream = kafkaClient.map(s -> {
            String[] split = s.split(",");
            return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim())).toString();
        });

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>("topic", new SimpleStringSchema(), properties);


        stream.addSink(myProducer);

        env.execute();
    }
}
