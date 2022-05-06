package cn.wscode.sink;

import cn.wscode.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author wangSheng
 * @title: FileSink
 * @projectName DS
 * @description: TODO
 * @date 2022/3/30 14:53
 */
public class FileSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //todo 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("input/clicks.csv");
        SingleOutputStreamOperator<Event> result = streamSource.flatMap((String line, Collector<Event> out) -> {
            String[] strings = line.split(", ");
            Event event = new Event(strings[0], strings[1], Long.parseLong(strings[2]));
            out.collect(event);
        }).returns(Event.class);

        //构建sink
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024 * 1024) //滚动大小
                        .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15)) //滚动间隔
                        .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5)) //不活跃间隔时间
                        .build())
                .build();
        streamSource.map(String::toString).addSink(fileSink);
        env.execute();
    }
}
