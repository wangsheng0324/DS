package cn.wscode.tableAsql;

import cn.wscode.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author wangSheng
 * @title: TableExample
 * @projectName DS
 * @description: TODO 创建表并转换表
 * @date 2022/4/13 17:37
 */
public class TableExample {
    public static void main(String[] args) throws Exception {
        //	获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //	读取数据源
        SingleOutputStreamOperator<Event> eventStream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 1000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L),
                new Event("Bob", "./prod?id=3", 90 * 1000L),
                new Event("Alice", "./prod?id=7", 105 * 1000L)
        );

        //	获取表环境 --》流式表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //  将数据流转换为表
        Table table = tableEnv.fromDataStream(eventStream);
        //  直接注册虚拟表
        tableEnv.createTemporaryView("event", eventStream);

        //todo 直接sql查询
        Table table1 = tableEnv.sqlQuery("select * from event");
        tableEnv.toDataStream(table1).print("t1");

        //todo 基于table直接转换
        Table table2 = table.select($("name"), $("url")).where($("name").isEqual("Bob"));
        //tableEnv.toDataStream(table2).print("t2");

        //todo 聚合转换    -->更新日志流 & 撤回流
        Table table3 = tableEnv.sqlQuery("select name,count(*) as cnt from " + table + " group by name");
        tableEnv.toChangelogStream(table3).print("t3");

        env.execute();

    }
}
