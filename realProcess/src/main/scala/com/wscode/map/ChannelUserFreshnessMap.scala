package com.wscode.map

import com.wscode.bean.{ChannelUserFreshness, Message, UserState}
import com.wscode.tools.TimeUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * Created by angel
 */
class ChannelUserFreshnessMap extends FlatMapFunction[Message, ChannelUserFreshness] {
  val hour: String = "yyyyMMddhh"
  val day: String = "yyyyMMdd"
  val month: String = "yyyyMM"

  override def flatMap(value: Message, out: Collector[ChannelUserFreshness]): Unit = {
    val timeStamp = value.timeStamp
    val hourTime = TimeUtils.getData(timeStamp, hour)
    val dayTime = TimeUtils.getData(timeStamp, day)
    val monthTime = TimeUtils.getData(timeStamp, month)
    val userScan = value.userScan
    val channelID = userScan.channelID
    val userID = userScan.userID

    //新鲜度，有历史数据的
    val userState = UserState.getUserState(userID, timeStamp)
    val isNew = userState.isNew
    val firstHour = userState.isFirstHour
    val firstDay = userState.isFirstDay
    val firstMonth = userState.isFirstMonth

    val channelUserFreshness = new ChannelUserFreshness
    channelUserFreshness.setTimeStamp(timeStamp)
    channelUserFreshness.setChannelID(channelID)

    //新增用户 ----> isNew-->true
    var newCount: Long = 0L
    if (isNew) {
      newCount = 1L
    }
    channelUserFreshness.setNewCount(newCount)

    //老用户 小时  天  月

    //小时
    var oldCount = 0L
    if (!userState.isNew && firstHour) {
      oldCount = 1L
    }
    channelUserFreshness.setOldCount(oldCount)
    channelUserFreshness.setDataField(hourTime)
    channelUserFreshness.setAggregateField(hourTime + channelID)
    out.collect(channelUserFreshness)

    //天
    oldCount = 0L
    if (!userState.isNew && firstDay) {
      oldCount = 1L
    }
    channelUserFreshness.setOldCount(oldCount)
    channelUserFreshness.setDataField(dayTime)
    channelUserFreshness.setAggregateField(dayTime + channelID)
    out.collect(channelUserFreshness)
    //月
    oldCount = 0L
    if (!userState.isNew && firstMonth) {
      oldCount = 1L
    }
    channelUserFreshness.setOldCount(oldCount)
    channelUserFreshness.setDataField(monthTime)
    channelUserFreshness.setAggregateField(monthTime + channelID)
    out.collect(channelUserFreshness)
  }
}
