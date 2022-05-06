package cn.wscode.window;




import cn.wscode.pojo.Event;
import cn.wscode.source.CustomSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * todo
 */

public class UvCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp));
        stream.print("input");
        // 需要按照url分组，开滑动窗口统计
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                // 同时传入增量聚合函数和全窗口函数
                .aggregate(new UvCountAgg(), new UvCountResult())
                .print();

        env.execute();
    }

    // 自定义增量聚合函数，来一条数据就加一
    public static class UvCountAgg implements AggregateFunction<Event, HashSet<String>, Long> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<String>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.name);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long)accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    // 自定义窗口处理函数，只需要包装窗口信息
    public static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long currentWatermark = context.currentWatermark();
            Long uv = elements.iterator().next();
            out.collect("窗口" + new Timestamp(start) + " ~ " + new Timestamp(start) + "中共有" + uv + "个元素，窗口闭合计算时，水位线处于：" + new Timestamp(currentWatermark));
        }
    }
}
