package cn.wscode.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author wangSheng
 * @title: UdfTest_TableFunction
 * @projectName DS
 * @description: TODO 自定义表函数
 * @date 2022/4/18 16:31
 */
public class UdfTest_TableFunction {
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
        tableEnv.createTemporarySystemFunction("mySplit", MySplit.class);

        // 调用UDF
        Table table = tableEnv.sqlQuery("select user_name,url, word, length from clickTable , LATERAL TABLE(mySplit(url)) as T(word, length)");

        tableEnv.toDataStream(table).print();

        env.execute();
    }

    // 自定义一个TableFunction，注意有泛型，这里输出的是两个字段，二元组
    public static class MySplit extends TableFunction<Tuple2<String, Integer>> {
        public void eval(String str){
            String[] fields = str.split("\\?");    // 转义问号，以及反斜杠本身
            for (String field : fields){
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
