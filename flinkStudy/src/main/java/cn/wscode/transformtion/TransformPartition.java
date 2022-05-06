package cn.wscode.transformtion;

import cn.wscode.pojo.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wangSheng
 * @title: TransformPartition
 * @projectName DS
 * @description: TODO 物理分区 随机分区&轮询分区&重缩放分区&广播&全局分区&自定义重分区
 * @date 2022/3/29 18:11
 */
public class TransformPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //todo 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("input/clicks.csv");
        SingleOutputStreamOperator<Event> flatMap = streamSource.flatMap((String line, Collector<Event> out) -> {
            String[] strings = line.split(", ");
            Event event = new Event(strings[0], strings[1], Long.parseLong(strings[2]));
            out.collect(event);
        }).setParallelism(2).returns(Event.class);

        //1、shuffle 随机分区
        //flatMap.shuffle().print().setParallelism(4);
        //2、rebalance 轮询分区 ==》默认
        //flatMap.rebalance().print().setParallelism(4);
        //3、rescale  重缩放分区
        //flatMap.rescale().print().setParallelism(4);
        //4、广播
        //flatMap.broadcast().print().setParallelism(4);
        //5、全局分区 并行度为1
        //flatMap.global().print();
        //6、自定义重分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {

                return key % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(2);
        env.execute();
    }
}
