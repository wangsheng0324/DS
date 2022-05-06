package com.wscode.map

import com.wscode.bean.{ChannelRegion, Message, UserState}
import com.wscode.tools.TimeUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * Created by angel
 */
class ChannelRegionMap extends FlatMapFunction[Message, ChannelRegion] {
  var hour: String = "yyyyMMddhh"
  var day: String = "yyyyMMdd"
  var month: String = "yyyyMM"

  override def flatMap(value: Message, out: Collector[ChannelRegion]): Unit = {
    //根据获取到的时间戳进行字符串截取，得到想要的时间格式
    val timeStamp = value.timeStamp
    val hourTime = TimeUtils.getData(timeStamp, hour)
    val dayTime = TimeUtils.getData(timeStamp, day)
    val monthTime = TimeUtils.getData(timeStamp, month)
    //获取用户的浏览记录
    val userScan = value.userScan
    val channelID = userScan.channelID
    val userID = userScan.userID
    val country = userScan.country
    val province = userScan.province
    val city = userScan.city
    //根据userID和timeStamp去Hbase查询，当前用户是不是新用户
    val userState = UserState.getUserState(userID, timeStamp)
    val isNew = userState.isNew
    val firstHour = userState.isFirstHour
    val firstDay = userState.isFirstDay
    val firstMonth = userState.isFirstMonth

    val channelRegion = new ChannelRegion
    channelRegion.setChannelID(channelID)
    channelRegion.setCountry(country)
    channelRegion.setProvince(province)
    channelRegion.setCity(city)
    channelRegion.setPV(1L)

    //新增用户
    isNew match {
      case true => channelRegion.setNewCount(1L)
      case _ => channelRegion.setNewCount(0L)
    }

    //小时
    firstHour match {
      case true =>
        channelRegion.setOldCount(1L)
        channelRegion.setUV(1L)
        channelRegion.setTimeStamp(timeStamp)
        channelRegion.setDataField(hourTime)
        channelRegion.setAggreagateField(hourTime + channelID + country + province + city)
        out.collect(channelRegion)
      case _ =>
        channelRegion.setOldCount(0L)
        channelRegion.setUV(0L)
        channelRegion.setTimeStamp(timeStamp)
        channelRegion.setDataField(hourTime)
        channelRegion.setAggreagateField(hourTime + channelID + country + province + city)
        out.collect(channelRegion)

    }
    //天
    firstDay match {
      case true =>
        channelRegion.setOldCount(1L)
        channelRegion.setUV(1L)
        channelRegion.setTimeStamp(timeStamp)
        channelRegion.setDataField(dayTime)
        channelRegion.setAggreagateField(dayTime + channelID  + country + province + city)
        out.collect(channelRegion)
      case _ =>
        channelRegion.setOldCount(0L)
        channelRegion.setUV(0L)
        channelRegion.setTimeStamp(timeStamp)
        channelRegion.setDataField(dayTime)
        channelRegion.setAggreagateField(dayTime + channelID  + country + province + city)
        out.collect(channelRegion)

    }
    //月
    firstMonth match {
      case true =>
        channelRegion.setOldCount(1L)
        channelRegion.setUV(1L)
        channelRegion.setTimeStamp(timeStamp)
        channelRegion.setDataField(monthTime)
        channelRegion.setAggreagateField(monthTime + channelID  + country + province + city)
        out.collect(channelRegion)
      case _ =>
        channelRegion.setOldCount(0L)
        channelRegion.setUV(0L)
        channelRegion.setTimeStamp(timeStamp)
        channelRegion.setDataField(monthTime)
        channelRegion.setAggreagateField(monthTime + channelID   + country + province + city)
        out.collect(channelRegion)

    }
  }
}
