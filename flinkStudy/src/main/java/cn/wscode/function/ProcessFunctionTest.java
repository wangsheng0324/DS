package cn.wscode.function;

import cn.wscode.pojo.Event;
import cn.wscode.source.CustomSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new CustomSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp)
                )
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (value.name.equals("Mary")) {
                            out.collect(value.name);
                        } else if (value.name.equals("Bob")) {
                            out.collect(value.name);
                            out.collect(value.name);
                        }
                        System.out.println(new Timestamp(ctx.timerService().currentWatermark()));
                    }
                })
                .print();

        env.execute();
    }
}

