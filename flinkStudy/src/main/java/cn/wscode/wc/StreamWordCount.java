package cn.wscode.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wangSheng
 * @title: StreamWordCount
 * @projectName DS
 * @description: TODO d
 * @date 2022/3/25 14:46
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1、创建流对象
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        //senv.setParallelism(2);
        DataStreamSource<String> file = senv.readTextFile("input/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = file.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneTuple.keyBy(word -> word.f0).sum(1);
        sum.print();
        //启动执行
        senv.execute();
    }
}
