package cn.wscode.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author wangSheng
 * @title: UdfTest_ScalarFunction
 * @projectName DS
 * @description: TODO 自定义标量函数
 * @date 2022/4/18 16:22
 */
public class UdfTest_ScalarFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("myHash", MyHashFunction.class);

        // 调用UDF
        Table table = tableEnv.sqlQuery("select user_name,myHash(user_name) as hash from clickTable");

        tableEnv.toDataStream(table).print();

        env.execute();


    }

    public static class MyHashFunction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
