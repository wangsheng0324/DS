package cn.wscode.tableAsql;

import cn.wscode.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wangSheng
 * @title: AppendQueryExample
 * @projectName DS
 * @description: TODO 追加查询 ==》使用滚动窗口
 * @date 2022/4/14 18:03
 */
public class AppendQueryExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> streamSource = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                .withTimestampAssigner((element, timestamp) -> element.timestamp));

        tableEnv.createTemporaryView("eventTable", streamSource, $("name"), $("url"), $("timestamp").rowtime().as("ts"));

        Table table = tableEnv.sqlQuery(
                "SELECT " +
                        "name, " +
                        "window_end AS endT, " +    // 窗口结束时间
                        "COUNT(url) AS cnt " +    // 统计 url 访问次数
                        "FROM TABLE( " +
                        "TUMBLE( TABLE eventTable, " +    // 1 小时滚动窗口
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '1' HOUR)) " +
                        "GROUP BY name, window_start, window_end "
        );
        tableEnv.toDataStream(table).print();
        env.execute();
    }
}
