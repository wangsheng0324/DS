package cn.wscode.tableAsql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author wangSheng
 * @title: CommonApi
 * @projectName DS
 * @description: TODO table的通用api
 * @date 2022/4/14 14:27
 */
public class CommonApi {
    public static void main(String[] args) throws Exception {
//        //	获取流执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        //	获取表环境 --》流式表环境
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.定义表的执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode() //处理模式（流&批）
                .useBlinkPlanner() //使用blink计划器
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);  //并行度当前的最大核数

        //2.表的创建
        String createDDl = "create table clickTable(" +
                "`name` String, " +
                "`url` String, " +
                "ts BIGINT) with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'E:\\maven_space\\DS\\flinkStudy\\input\\clicks.csv', " +
                " 'format' = 'csv')";
        tableEnv.executeSql(createDDl);

        //创建输出表
        String createOutDDl = "create table clickOutTable(" +
                "`name` String, " +
                "`url` String) with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'E:\\maven_space\\DS\\flinkStudy\\output', " +
                " 'format' = 'csv')";

        tableEnv.executeSql(createOutDDl);

        Table table = tableEnv.sqlQuery("select name,url from clickTable");

        //table.executeInsert("clickOutTable");

        //控制台输出
        String printOutTable = "create table printOutTable(" +
                "`name` String, " +
                "`url` String) with (" +
                " 'connector' = 'print')" ;
        tableEnv.executeSql(printOutTable);
        table.executeInsert("printOutTable");


    }
}
