package cn.wscode.cep;

import cn.wscode.pojo.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author wangSheng
 * @title: LoginFailDetectPro
 * @projectName DS
 * @description: TODO 登录失败检测 ==》 高阶
 * @date 2022/4/19 15:47
 */
public class LoginFailDetectPro {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 获取登录事件流，并提取时间戳、生成水位线
//        KeyedStream<LoginEvent, String> stream = env
//                .fromElements(
//                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
//                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
//                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
//                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
//                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
//                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
//                        new LoginEvent("user_2", "192.168.1.30", "success", 6000L),
//                        new LoginEvent("user_3", "192.168.1.30", "fail", 47000L),
//                        new LoginEvent("user_3", "192.168.1.30", "fail", 48000L),
//                        new LoginEvent("user_3", "192.168.1.30", "fail", 18000L), //迟到数据
//                        new LoginEvent("user_3", "192.168.1.30", "success", 50000L),
//                        new LoginEvent("user_4", "192.168.1.31", "fail", 55000L),
//                        new LoginEvent("user_4", "192.168.1.31", "fail", 57000L),
//                        new LoginEvent("user_4", "192.168.1.31", "fail", 59000L)
//                )
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                                .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (loginEvent, l) -> loginEvent.timestamp))
//                .keyBy(r -> r.userId);

        KeyedStream<LoginEvent, String> stream = env.socketTextStream("101.200.47.4", 9091).map(r -> {
            String[] split = r.split(", ");
            return new LoginEvent(split[0], split[1], split[2], Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (loginEvent, l) -> loginEvent.timestamp)).keyBy(r -> r.userId);
        stream.print();
        // 2. 定义Pattern，连续的三个登录失败事件，并输出报警信息
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        }).times(3).consecutive() // times()默认是宽松近邻关系 consecutive()默认是连续关系 -->严格近邻关系
                .within(Time.seconds(10)); // 组合时间间隔

        // 3. 将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);
        // 迟到数据测输出流
        OutputTag<LoginEvent> lateDataOutputTag = new OutputTag<LoginEvent>("late-data") {
        };

        // 4. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        SingleOutputStreamOperator<String> select = patternStream.sideOutputLateData(lateDataOutputTag).select((PatternSelectFunction<LoginEvent, String>) map -> {

            LoginEvent first = map.get("fail").get(0);
            LoginEvent second = map.get("fail").get(1);
            LoginEvent third = map.get("fail").get(2);
            return first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
        });

        select.print("data:");
        select.getSideOutput(lateDataOutputTag).print("late-data:");

        env.execute();

    }
}
