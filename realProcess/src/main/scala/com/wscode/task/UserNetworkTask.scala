package com.wscode.task

import com.wscode.`trait`.DataProcess
import com.wscode.bean.{Message, UserNetwork}
import com.wscode.map.UserNetworkMap
import com.wscode.reduce.UserNetworkReduce
import com.wscode.sink.UserNetworkSink
import com.wscode.task.ChannelUserFreshnessTask.resultAtomic
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
object UserNetworkTask extends DataProcess {
  var resultAtomic: Int = 0

  override def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int = {
    try {
      //1）：将水印数据转换成UserNetwork数据
      val mapData: DataStream[UserNetwork] = watermarkData.flatMap(new UserNetworkMap)
      //2）：分流
      val keyByData: KeyedStream[UserNetwork, String] = mapData.keyBy(line => line.getDataField + line.getNetwork)
      //3）：时间窗口划分
      val window: WindowedStream[UserNetwork, String, TimeWindow] = keyByData.timeWindow(Time.seconds(1))
      //4）：指标数据聚合
      val result = window.reduce(new UserNetworkReduce)
      //5）：数据指标落地
      result.addSink(new UserNetworkSink)
      resultAtomic = atomic.incrementAndGet()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        atomic.decrementAndGet()
    }
    resultAtomic
  }
}
