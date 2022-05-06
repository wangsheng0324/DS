package com.wscode.map

import com.wscode.bean.{ChannelPVUV, Message, UserState}
import com.wscode.tools.TimeUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * Created by angel
 */
class ChannelPVUVMap extends FlatMapFunction[Message, ChannelPVUV] {
  override def flatMap(value: Message, out: Collector[ChannelPVUV]): Unit = {
    val timeStamp = value.timeStamp
    //根据时间戳获取聚合的字段
    val month = TimeUtils.getData(timeStamp, "yyyyMM")
    val day = TimeUtils.getData(timeStamp, "yyyyMMdd")
    val hour = TimeUtils.getData(timeStamp, "yyyyMMddHH")

    val userScan = value.userScan
    val userID = userScan.userID
    //根据用户ID和时间戳获取用户访问状态
    val userState = UserState.getUserState(userID, timeStamp)
    val firstHour = userState.isFirstHour
    val firstDay = userState.isFirstDay
    val firstMonth = userState.isFirstMonth

    val channelID = userScan.channelID
    val channelPVUV = new ChannelPVUV
    channelPVUV.setChannelID(channelID)
    channelPVUV.setUserID(userID)
    val pvCount = value.count
    channelPVUV.setPV(pvCount)
    //小时
    firstHour match {
      case true => channelPVUV.setUV(1L)
      case _ => channelPVUV.setUV(0L)
    }
    //rowkey的拼接字段
    channelPVUV.setTimeStamp(timeStamp)
    channelPVUV.setDateField(hour)
    channelPVUV.setAggregateField(hour + channelID)
    out.collect(channelPVUV)
    //天
    firstDay match {
      case true => channelPVUV.setUV(1L)
      case _ => channelPVUV.setUV(0L)
    }
    //rowkey的拼接字段
    channelPVUV.setTimeStamp(timeStamp)
    channelPVUV.setDateField(day)
    channelPVUV.setAggregateField(day + channelID)
    out.collect(channelPVUV)
    //月
    firstMonth match {
      case true => channelPVUV.setUV(1L)
      case _ => channelPVUV.setUV(0L)
    }
    //rowkey的拼接字段
    channelPVUV.setTimeStamp(timeStamp)
    channelPVUV.setDateField(month)
    channelPVUV.setAggregateField(month + channelID)
    out.collect(channelPVUV)

  }
}
