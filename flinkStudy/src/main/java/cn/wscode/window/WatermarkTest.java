package cn.wscode.window;

import cn.wscode.pojo.Event;
import cn.wscode.source.CustomSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author wangSheng
 * @title: WatermarkTest
 * @projectName DS
 * @description: TODO 全窗口函数 ProcessWindowFunction
 * @date 2022/3/31 15:23
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 将数据源改为socket文本流，并转换成Event类型
        // 抽取时间戳的逻辑
        env.addSource(new CustomSource())
                // 插入水位线的逻辑
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为2s
                        //forMonotonousTimestamps 有序流
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp))
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("水位线"+new Timestamp(ctx.timerService().currentWatermark())+" ： "+value.toString());

                    }
                }).print();
//                // 根据user分组，开窗统计
//                .keyBy(data ->true)
//                //滚动事件时间窗口
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .process(new WatermarkTestResult())
//                .print();

        env.execute();
    }

    // 自定义处理窗口函数，输出当前的水位线和窗口信息
    public static class WatermarkTestResult extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Long currentWatermark = context.currentWatermark();
            Long count = elements.spliterator().getExactSizeIfKnown();
            out.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
        }
    }
}
