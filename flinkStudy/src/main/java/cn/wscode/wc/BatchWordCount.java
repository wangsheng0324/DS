package cn.wscode.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author wangSheng
 * @title: WordCount
 * @projectName DS
 * @description: TODO flink 的 批处理wordCount 入门 dataSetAPI 面临淘汰
 * @date 2022/3/24 16:52
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2、读取文件
        DataSource<String> lineDS = env.readTextFile("input/words.txt");
        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOne.groupBy(0).sum(1);

        sum.print();

    }
}
