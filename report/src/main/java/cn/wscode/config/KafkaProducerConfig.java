package cn.wscode.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * todo：kafka 转化为springboot的 KafkaTemplate
 */
@Configuration
@EnableKafka
public class KafkaProducerConfig {
    @Value("${kafka.producer.server}")
    private String servers ;
    @Value("${kafka.producer.retries}")
    private int retries ;
    @Value("${kafka.producer.batch.size}")
    private int batchSize ;
    @Value("${kafka.producer.linger}")
    private int linger ;
    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory ;

    @Value("${kafka.producer.partitioner}")
    private String partitioner ;

    /**
     * 将默认的分区方式转化成roundrobin模式(轮询)
     */
    public Map<String , Object> producerConfig(){
        Map<String , Object> map = new HashMap<String , Object>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , servers);
        map.put(ProducerConfig.RETRIES_CONFIG , retries);
        map.put(ProducerConfig.LINGER_MS_CONFIG , linger);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG , batchSize);
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG , bufferMemory);
        map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG , partitioner);
        //序列化的配置
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        return map ;
    }
    //获取kafka的配置函数工厂
    public ProducerFactory<String , String> producerFactory(){
        return new DefaultKafkaProducerFactory<String, String>(producerConfig());
    }

    //对springboot提供服务的函数
    @Bean
    public KafkaTemplate<String , String> kafkaTemplate(){
        return new KafkaTemplate<String, String>(producerFactory());
    }
}
