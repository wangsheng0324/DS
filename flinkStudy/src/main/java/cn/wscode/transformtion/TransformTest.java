package cn.wscode.transformtion;

import cn.wscode.pojo.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wangSheng
 * @title: TransformTest
 * @projectName DS
 * @description: TODO 算子
 * @date 2022/3/29 14:34
 */
public class TransformTest {
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
//        flatMap.print("flatMap");
        SingleOutputStreamOperator<String> map = flatMap.map(event -> event.name);
        SingleOutputStreamOperator<String> filter = map.filter(name -> name.equals("Bob"));
        //filter.print();

        // todo reduce 规约聚合 两两规约 输出与输入保持一致
        KeyedStream<Tuple2<String, Long>, String> reduceData = flatMap.map(s -> Tuple2.of(s.name, 1L)).returns(new TypeHint<Tuple2<String, Long>>() {
        }).keyBy(s -> s.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = reduceData.reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));
        clickByUser.keyBy(s -> "key").reduce((t1, t2) -> t1.f1 > t2.f1 ? t1 : t2).print();


        env.execute();
    }
}
