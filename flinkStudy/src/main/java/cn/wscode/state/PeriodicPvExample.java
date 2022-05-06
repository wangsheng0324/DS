package cn.wscode.state;

import cn.wscode.pojo.Event;
import cn.wscode.source.CustomSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author wangSheng
 * @title: PeriodicPvExample
 * @projectName DS
 * @description: TODO 周期性计算pv
 * @date 2022/4/11 16:46
 */
public class PeriodicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, timestamp) -> element.timestamp));

        stream.keyBy(Event::getName)
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    private static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
        //定义状态
        ValueState<Long> countState;
        //定时器
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Long.class));
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            countState.update(countState.value() == null ? 1 : countState.value() + 1);

            //注册定时器
            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.timestamp + 1000L * 10);
                timerTsState.update(value.timestamp + 1000L * 10);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出结果
            out.collect(ctx.getCurrentKey() + " pv: " + countState.value());

            //清空定时器状态
            timerTsState.clear();
        }
    }
}
