package cn.wscode.state;

import cn.wscode.pojo.Event;
import cn.wscode.source.CustomSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

/**
 * @author wangSheng
 * @title: BufferingSinkExample
 * @projectName DS
 * @description: TODO 数据缓存输出 使用后检查点
 * @date 2022/4/12 14:40
 */
public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置检查点
        env.enableCheckpointing(10000L);
        //todo 状态后端：HashMap哈希表 --本地内存 & Embedded内嵌RocksDB -- 本地硬盘化
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());


        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("")); // 指定检查点存储位置
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 模式 精确一次 & 至少一次
        checkpointConfig.setMinPauseBetweenCheckpoints(500); // 检查点间隔时间
        checkpointConfig.setCheckpointTimeout(60000); // 检查点超时时间
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 并发检查点数量
        checkpointConfig.enableExternalizedCheckpoints( // 外部持久化
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消任务时保留检查点
        checkpointConfig.enableUnalignedCheckpoints(); // 允许不对齐的检查点
        checkpointConfig.setTolerableCheckpointFailureNumber(0); // 允许的检查点失败数量


        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                );

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    //自定义sink 并实现检查点
    private static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        //定义批量数
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
        }

        private List<Event> bufferElements;
        //定义算子状态
        private ListState<Event> checkpointedState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            //缓存到列表
            bufferElements.add(value);
            //判断是否达到批量数
            if (bufferElements.size() >= threshold) {

                for (Event element : bufferElements) {
                    System.out.println(element);
                }
                //达到批量数后，清空缓存列表
                bufferElements.clear();
            }
        }

        //快照 --》对状态进行持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //清空历史状态
            checkpointedState.clear();
            checkpointedState.addAll(bufferElements);
        }

        //初始化
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("buffering-sink", Event.class));

            //如果故障恢复，则从状态中恢复数据
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()) {
                    bufferElements.add(event);
                }
            }
        }
    }
}
