package cn.wscode.tableAsql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author wangSheng
 * @title: TopNExample
 * @projectName DS
 * @description: TODO flink 实现topN
 * @date 2022/4/15 18:24
 */
public class TopNExample {
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

        // 普通TopN
        // 选取当前最大的两个
        Table maxTable = tableEnv.sqlQuery("select user_name,cnt,row_num " +
                "from (" +
                "select *,row_number() over (order by cnt desc) as row_num " +
                "from (select user_name,count(*) as cnt from clickTable " +
                "group by user_name )" +
                ") where row_num <= 2");

        //tableEnv.toChangelogStream(maxTable).print("maxTable");


        // 窗口TopN ,统计一段时间的前两位活跃用户
        Table windowTopNResult = tableEnv.sqlQuery("select user_name,cnt,row_num,window_end " +
                "from (" +
                "select *,row_number() over (partition by window_start,window_end order by cnt desc) as row_num " +
                "from (select user_name,count(*) as cnt,window_start,window_end  " +
                "from TABLE( " +
                " TUMBLE( " +
                " TABLE  clickTable," +
                " descriptor(et)," +
                " INTERVAL '10' SECOND)) " +
                "group by user_name,window_start,window_end )" +
                ") where row_num <= 2");
        tableEnv.toDataStream(windowTopNResult).print("windowTopNResult");
        env.execute();

    }
}
