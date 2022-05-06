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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author wangSheng
 * @title: LoginFailDetect
 * @projectName DS
 * @description: TODO 登录失败检测
 * @date 2022/4/19 11:10
 */
public class LoginFailDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<LoginEvent, String> stream = env
                .fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((SerializableTimestampAssigner<LoginEvent>) (loginEvent, l) -> loginEvent.timestamp))
                .keyBy(r -> r.userId);

        // 2. 定义Pattern，连续的三个登录失败事件，并输出报警信息
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        }).next("second").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        }).next("third").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.eventType.equals("fail");
            }
        });

        // 3. 将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<LoginEvent> patternStream = CEP.pattern(stream, pattern);

        // 4. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        patternStream.select((PatternSelectFunction<LoginEvent, String>) map -> {

            LoginEvent first = map.get("first").get(0);
            LoginEvent second = map.get("second").get(0);
            LoginEvent third = map.get("third").get(0);
            return first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
        }).print();
        
        env.execute();

    }
}
