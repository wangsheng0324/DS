package com.wscode.task

import com.wscode.`trait`.DataProcess
import com.wscode.bean.{ChannelRegion, Message}
import com.wscode.map.ChannelRegionMap
import com.wscode.reduce.ChannelRegionReduce
import com.wscode.sink.ChannelRegionSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
object ChannelRegionTask extends DataProcess {
  var resultAtomic: Int = 0

  override def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int = {
    try {
      //1）：通过水印数据封装成频道地域数据
      val mapData: DataStream[ChannelRegion] = watermarkData.flatMap(new ChannelRegionMap)
      //2):分流
      val keyByData = mapData.keyBy(line => line.getAggregateField)
      //3):时间窗口划分
      val window: WindowedStream[ChannelRegion, String, TimeWindow] = keyByData.timeWindow(Time.seconds(3))
      //4)：进行指标的聚合操作---pvuv  新鲜度
      val result = window.reduce(new ChannelRegionReduce)
      //5）：指标落地
      result.addSink(new ChannelRegionSink)
      resultAtomic = atomic.incrementAndGet()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        atomic.decrementAndGet()
    }
    resultAtomic
  }
}
