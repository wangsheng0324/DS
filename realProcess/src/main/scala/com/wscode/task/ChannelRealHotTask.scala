package com.wscode.task

import com.wscode.`trait`.DataProcess
import com.wscode.bean.{ChannelRealHot, Message}
import com.wscode.map.RealHotMap
import com.wscode.reduce.ChannelRealHotReduce
import com.wscode.sink.ChannelRealHotSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
object ChannelRealHotTask extends DataProcess {
  var resultAtomic: Int = 0

  override def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int = {
    /**
     * 1)：将实时解析出的数据转换成频道热点实体类
     *
     * 2）：分流（分组）
     *
     * 3）：时间窗口的划分
     *
     * 4）： 对频道的点击数进行聚合操作
     *
     * 5）：将结果数据进行落地操作
     * */
    try {
      //将实时解析出的数据转换成频道热点实体类
      val channelRealHot: DataStream[ChannelRealHot] = watermarkData.flatMap(new RealHotMap)
      //分流（分组）
      val keyByData: KeyedStream[ChannelRealHot, String] = channelRealHot.keyBy(line => line.getChannelID)
      //时间窗口的划分
      val window: WindowedStream[ChannelRealHot, String, TimeWindow] = keyByData.timeWindow(Time.seconds(3))
      //对频道的点击数进行聚合操作
      val reduceData: DataStream[ChannelRealHot] = window.reduce(new ChannelRealHotReduce)
      //将结果数据进行落地操作--->hbase
      reduceData.addSink(new ChannelRealHotSink)
      resultAtomic = atomic.incrementAndGet()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        atomic.decrementAndGet()
    }
    resultAtomic
  }
}
