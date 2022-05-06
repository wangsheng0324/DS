package com.wscode.task

import com.wscode.`trait`.DataProcess
import com.wscode.bean.{Message, UserBrowser}
import com.wscode.map.UserBrowserMap
import com.wscode.reduce.UserBrowserReduce
import com.wscode.sink.UserBrowserSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
object UserBrowserTask extends DataProcess {
  var resultAtomic: Int = 0

  override def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int = {
    try {
      //1):根据水印数据转换出：用户浏览器数据
      val mapData: DataStream[UserBrowser] = watermarkData.flatMap(new UserBrowserMap)
      //2）：数据分流
      val keyBydata: KeyedStream[UserBrowser, String] = mapData.keyBy(line => line.getDataField + line.getBrowser)
      //3):时间窗口划分
      val window: WindowedStream[UserBrowser, String, TimeWindow] = keyBydata.timeWindow(Time.seconds(3))
      //4):指标聚合
      val result = window.reduce(new UserBrowserReduce)
      //5）：数据指标落地
      result.addSink(new UserBrowserSink)
      resultAtomic = atomic.incrementAndGet()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        atomic.decrementAndGet()
    }
    resultAtomic
  }
}
