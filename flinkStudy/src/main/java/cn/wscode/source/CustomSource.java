package cn.wscode.source;

import cn.wscode.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;

/**
 * @author wangSheng
 * @title: CustomSource
 * @projectName DS
 * @description: TODO 自定义数据源
 * 实现 ParallelSourceFunction 自定义数据源才可容易提高并行度
 * @date 2022/3/29 11:30
 */
public class CustomSource implements SourceFunction<Event> {
    //声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary", "Tom"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=10", "./prod?id=99"};
        while (running) {
            ctx.collect(new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], Calendar.getInstance().getTimeInMillis()));
            //SourceContext 可以发送水位线
//            ctx.collectWithTimestamp(event，event.timestamp );
//            ctx.emitWatermark(new Watermark(event.timestamp -1L));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
