package cn.wscode.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author wangSheng
 * @title: UdfTest_AggregateFunction
 * @projectName DS
 * @description: TODO 自定义聚合函数
 * @date 2022/4/18 16:57
 */
public class UdfTest_AggregateFunction {
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
        tableEnv.createTemporarySystemFunction("weightedAverage", WeightedAverage.class);

        // 调用UDF
        Table table = tableEnv.sqlQuery("select user_name, " +
                "  WeightedAverage(ts, 1) as weighted_avg " +
                "from clickTable " +
                "group by user_name");

        tableEnv.toChangelogStream(table).print();

        env.execute();
    }

    // 单独定义一个累加器类型
    public static class WeightedAvgAccumulator {
        public long sum = 0;    // 加权和
        public int count = 0;    // 数据个数
    }

    // 自定义一个AggregateFunction，求加权平均值
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0)
                return null;    // 防止除数为0
            else
                return accumulator.sum / accumulator.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        // 累加计算方法，类似于add
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer iWeight) {
            accumulator.sum += iValue * iWeight;    // 这个值要算iWeight次
            accumulator.count += iWeight;
        }

    }
}
