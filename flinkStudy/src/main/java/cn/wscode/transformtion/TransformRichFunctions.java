package cn.wscode.transformtion;

import cn.wscode.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author wangSheng
 * @title: TransformRichFunctions
 * @projectName DS
 * @description: TODO 富函数的实现 ==》可以获取运行环境的上下文，并拥有一些生命周期方法
 * @date 2022/3/29 17:25
 */
public class TransformRichFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> listStream = env.fromCollection(
                Arrays.asList(
                        new Event("Mary", "./home", 1000L),
                        new Event("Boy", "./cart", 2000L)
                )
        );
        SingleOutputStreamOperator<Integer> map = listStream.map(new RichMapFunction<Event, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("富函数的open生命周期被调用，" + getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public Integer map(Event event) throws Exception {

                return event.url.length();
            }


            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("富函数的close生命周期被调用，" + getRuntimeContext().getIndexOfThisSubtask());
            }
        }).setParallelism(1);
        map.print();

        env.execute();
    }
}
