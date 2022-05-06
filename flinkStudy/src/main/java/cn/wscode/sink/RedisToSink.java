package cn.wscode.sink;

import cn.wscode.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @author wangSheng
 * @title: RedisSink
 * @projectName DS
 * @description: TODO
 * @date 2022/3/30 14:53
 */
public class RedisToSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //todo 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("input/clicks.csv");
        SingleOutputStreamOperator<Event> flatMap = streamSource.flatMap((String line, Collector<Event> out) -> {
            String[] strings = line.split(", ");
            Event event = new Event(strings[0], strings[1], Long.parseLong(strings[2]));
            out.collect(event);
        }).returns(Event.class);

        RedisSink<Event> mySink = new RedisSink<>(new FlinkJedisPoolConfig.Builder().setHost("localhost").build(), new RedisMapper<Event>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET);
            }

            @Override
            public String getKeyFromData(Event event) {
                return event.name;
            }

            @Override
            public String getValueFromData(Event event) {
                return event.url;
            }
        });

        flatMap.addSink(mySink);
        env.execute();

    }
}
