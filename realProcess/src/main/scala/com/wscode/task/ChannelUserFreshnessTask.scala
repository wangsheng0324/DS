package com.wscode.task

import com.wscode.`trait`.DataProcess
import com.wscode.bean.{ChannelUserFreshness, Message}
import com.wscode.map.ChannelUserFreshnessMap
import com.wscode.reduce.ChannelUserFreshnessReduce
import com.wscode.sink.ChannelUserFreshnessSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
object ChannelUserFreshnessTask extends DataProcess {
  var resultAtomic: Int = 0

  override def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int = {
    try {
      //1）：根据传递过来的水印数据解析出用户新鲜度数据 meesage ---> ChannelUserFreshness
      val mapData: DataStream[ChannelUserFreshness] = watermarkData.flatMap(new ChannelUserFreshnessMap)
      //2):数据分流操作
      val keybyData: KeyedStream[ChannelUserFreshness, String] = mapData.keyBy(line => line.getAggregateField)
      //3): 划分时间窗口
      val timeData: WindowedStream[ChannelUserFreshness, String, TimeWindow] = keybyData.timeWindow(Time.seconds(3))
      //4): 对新鲜度指标进行聚合操作
      val result: DataStream[ChannelUserFreshness] = timeData.reduce(new ChannelUserFreshnessReduce)
      //5）：将结果落地到hbase中
      result.addSink(new ChannelUserFreshnessSink)
      resultAtomic = atomic.incrementAndGet()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        atomic.decrementAndGet()
    }
    resultAtomic
  }
}
