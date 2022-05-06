package cn.wscode.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangSheng
 * @title: RoundRobinPartitioner
 * @projectName DS
 * @description: TODO 均匀发送各个分区的消息
 * @date 2022/4/26 16:02
 */
public class RoundRobinPartitioner implements Partitioner {
    // 线程安全的计数器
    final AtomicInteger counter = new AtomicInteger(0);
    int partitionNum = 3 ;

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        final  int partitionId = counter.incrementAndGet() % partitionNum;
        //线程最大数
        if (counter.get() > 65536){
            counter.set(0);
        }
        return partitionId;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
