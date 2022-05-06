package cn.wscode.source;

import cn.wscode.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author wangSheng
 * @title: CollectionSource
 * @projectName DS
 * @description: TODO 有界流读取数据
 * @date 2022/3/28 22:17
 */
public class DataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置模式
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        //todo 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("input/clicks.csv");
        SingleOutputStreamOperator<Event> result = streamSource.flatMap((String line, Collector<Event> out) -> {
            String[] strings = line.split(", ");
            Event event = new Event(strings[0], strings[1], Long.parseLong(strings[2]));
            out.collect(event);
        }).returns(Event.class);

        //todo 读取集合
        ArrayList<Integer> list = new ArrayList<>();
        list.add(7);
        list.add(9);
        list.add(8);
        DataStreamSource<Integer> listStream1 = env.fromCollection(list);

        DataStreamSource<Event> listStream2 = env.fromCollection(
                Arrays.asList(
                        new Event("Mary", "./home", 1000L),
                        new Event("Boy", "./cart", 2000L)
                )
        );

        //todo 读取元素
        DataStream<Integer> intStream = env.fromElements(1, 2, 3);

        //todo socket服务获取文本
        DataStreamSource<String> socketStream = env.socketTextStream("101.200.47.4", 9091);

//        result.print("fileStream");
//        listStream1.print("listStream1");
//        listStream2.print("listStream2");
//        intStream.print("intStream");
//        socketStream.print("socketStream");

        //todo 自定义数据源
        DataStreamSource<Event> customStream = env.addSource(new CustomSource());
        customStream.print("customStream");

        env.execute();

    }
}
