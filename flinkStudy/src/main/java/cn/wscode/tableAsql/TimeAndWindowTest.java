package cn.wscode.tableAsql;

import cn.wscode.pojo.Event;
import cn.wscode.source.CustomSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wangSheng
 * @title: TimeAndWindowTest
 * @projectName DS
 * @description: TODO 时间和窗口测试
 * @date 2022/4/15 15:07
 */
public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'E:\\maven_space\\DS\\flinkStudy\\input\\clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        Table table = tableEnv.sqlQuery("select * from clickTable");
        // tableEnv.toDataStream(table).print();

        // 2.流转换成table时指定时间语义
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp));
        // rowtime => 事件时间 proctime => 处理时间
        Table clickTable = tableEnv.fromDataStream(clickStream, $("name"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());

        // 聚合查询转换
        // 1.分组聚合
        Table aggTable = tableEnv.sqlQuery("select user_name, count(*) from clickTable group by user_name");

        // 2.分组窗口聚合
        Table oldWinTable = tableEnv.sqlQuery("select " +
                " user_name,  count(*) as cnt, " +
                " TUMBLE_END(et, INTERVAL '10' SECOND) as endTime " +
                " from clickTable " +
                " group by user_name, TUMBLE(et, INTERVAL '10' SECOND)");


        // 3.窗口聚合
        // 3.1 滚动窗口
        Table newWinTable = tableEnv.sqlQuery("select " +
                " user_name,  count(*) as cnt,window_end as  endTime" +
                " from TABLE( " +
                " TUMBLE( " +
                " TABLE  clickTable," +
                " descriptor(et)," +
                " INTERVAL '10' SECOND)) " +
                " group by user_name,window_start, window_end");
        // 3.2 滑动窗口
        Table hopWinTable = tableEnv.sqlQuery("select " +
                " user_name,  count(*) as cnt,window_end as  endTime" +
                " from TABLE( " +
                " HOP( " +
                " TABLE  clickTable," +
                " descriptor(et)," +
                " Interval '5' SECOND, " +
                " INTERVAL '10' SECOND)) " +
                " group by user_name,window_start, window_end");

        // 3.3 累计窗口
        Table cumWinTable = tableEnv.sqlQuery("select " +
                " user_name,  count(*) as cnt,window_end as  endTime" +
                " from TABLE( " +
                " CUMULATE( " +
                " TABLE  clickTable," +
                " descriptor(et)," +
                " Interval '5' SECOND, " +
                " INTERVAL '10' SECOND)) " +
                " group by user_name,window_start, window_end");

        // 4.开窗聚合 -》 over聚合    范围间隔 （RANGE BETWEEN） & 行间隔（rows between）
        Table overWinTable = tableEnv.sqlQuery("select " +
                " user_name," +
                " avg(ts) over (partition by user_name order by  et rows between 3 preceding and current row) as avg_ts " +
                " from clickTable");


//        tableEnv.toChangelogStream(aggTable).print("aggTable");
//        tableEnv.toDataStream(oldWinTable).print("oldWinTable");
//        tableEnv.toDataStream(newWinTable).print("newWinTable");
        //tableEnv.toDataStream(hopWinTable).print("hopWinTable");
        //tableEnv.toDataStream(cumWinTable).print("cumWinTable");
        tableEnv.toDataStream(overWinTable).print("overWinTable");


        env.execute();
    }
}
