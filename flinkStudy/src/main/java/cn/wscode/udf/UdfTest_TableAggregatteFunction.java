package cn.wscode.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author wangSheng
 * @title: UdfTest_TableAggregatteFunction
 * @projectName DS
 * @description: TODO 自定义表聚合函数
 * @date 2022/4/18 17:30
 */
public class UdfTest_TableAggregatteFunction {
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
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        // 调用UDF
        Table windowAggTable = tableEnv.sqlQuery("select user_name, count(url) as cnt, " +
                "window_end " +
                "from TABLE(" +
                "  TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND )" +
                ")" +
                "group by user_name," +
                "  window_start," +
                "  window_end");
        tableEnv.createTemporaryView("AggTable", windowAggTable);

        // 在Table API中调用函数
        Table resultTable = tableEnv.from("AggTable")
                .groupBy($("window_end"))
                .flatAggregate(call("Top2", $("cnt")).as("value", "rank"))
                .select($("window_end"), $("value"), $("rank"));

        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }
    // 聚合累加器的类型定义，包含最大的第一和第二两个数据
    public static class Top2Accumulator {
        public Long first;
        public Long second;
    }

    // 自定义表聚合函数，查询一组数中最大的两个，返回值为(数值，排名)的二元组
    public static class Top2 extends TableAggregateFunction<Tuple2<Long,Integer>,Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Long.MIN_VALUE;    // 为方便比较，初始值给最小值
            acc.second = Long.MIN_VALUE;
            return acc;
        }
        // 每来一个数据调用一次，判断是否更新累加器
        public void accumulate(Top2Accumulator acc, Long value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // 输出(数值，排名)的二元组，输出两行数据
        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Long, Integer>> out) {
            if (acc.first != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Long.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }

    }
}
