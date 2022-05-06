package cn.wscode.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wangSheng
 * @title: SocketStream
 * @projectName DS
 * @description: TODO flink 的  无界流处理wordCount 入门
 * @date 2022/3/25 17:32
 */
public class SocketStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //参数中提取主机&端口
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");

        int port = parameterTool.getInt("port");
        System.out.println(host + ":" + port);
        //监听socket端口
        DataStreamSource<String> dataSource = senv.socketTextStream("101.200.47.4", 9091);
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2s = dataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2s.keyBy(word -> word.f0).sum(1);
        sum.print();
        senv.execute();

    }
}
