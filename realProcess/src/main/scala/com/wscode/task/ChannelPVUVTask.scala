package com.wscode.task

import com.wscode.`trait`.DataProcess
import com.wscode.bean.{ChannelPVUV, Message}
import com.wscode.map.ChannelPVUVMap
import com.wscode.reduce.ChannelPVUVReduce
import com.wscode.sink.ChannelPVUVSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
object ChannelPVUVTask extends DataProcess {
  var resultAtomic:Int = 0

  override def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int = {
    try {
      //根据水印数据获取PVUV实体类
      val pvuvMapData: DataStream[ChannelPVUV] = watermarkData.flatMap(new ChannelPVUVMap)
      //将数据进行分流
      val groupData: KeyedStream[ChannelPVUV, String] = pvuvMapData.keyBy(line => line.getAggregateField)
      //时间窗口划分
      val window: WindowedStream[ChannelPVUV, String, TimeWindow] = groupData.timeWindow(Time.seconds(3))
      //将数据进行聚合
      val result: DataStream[ChannelPVUV] = window.reduce(new ChannelPVUVReduce)
      result.addSink(new ChannelPVUVSink)
      resultAtomic = atomic.incrementAndGet()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        atomic.decrementAndGet()
    }
    resultAtomic
  }
}
