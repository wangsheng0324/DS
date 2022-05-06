package com.wscode.`trait`

import com.wscode.bean.Message
import org.apache.flink.streaming.api.scala.DataStream

import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by angel
 */
trait DataProcess {
  def process(watermarkData: DataStream[Message], atomic: AtomicInteger): Int
}
