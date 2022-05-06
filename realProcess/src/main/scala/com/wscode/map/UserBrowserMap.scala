package com.wscode.map

import com.wscode.bean.{Message, UserBrowser, UserState}
import com.wscode.tools.TimeUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * Created by angel
 */
class UserBrowserMap extends FlatMapFunction[Message, UserBrowser] {
  val hour: String = "yyyyMMddhh"
  val day: String = "yyyyMMdd"
  val month: String = "yyyyMM"

  override def flatMap(value: Message, out: Collector[UserBrowser]): Unit = {
    val timeStamp = value.timeStamp
    val hourTime = TimeUtils.getData(timeStamp, hour)
    val dayTime = TimeUtils.getData(timeStamp, day)
    val monthTime = TimeUtils.getData(timeStamp, month)

    val userScan = value.userScan
    val userID = userScan.userID
    val browserType = userScan.browserType
    //userid ,timeStamp ---> userState
    val userState = UserState.getUserState(userID, timeStamp)
    val isNew = userState.isNew
    val firstHour = userState.isFirstHour
    val firstDay = userState.isFirstDay
    val firstMonth = userState.isFirstMonth

    val userBrowser = new UserBrowser
    userBrowser.setBrowser(browserType)
    userBrowser.setTimeStamp(timeStamp)
    userBrowser.setCount(1L)

    //新用户
    isNew match {
      case true =>
        userBrowser.setNewCount(1L)
      case _ =>
        userBrowser.setNewCount(0L)
    }

    //小时
    firstHour match {
      case true =>
        userBrowser.setOldCount(1L)
        userBrowser.setDataField(hourTime)
        out.collect(userBrowser)
      case _ =>
        userBrowser.setOldCount(0L)
        userBrowser.setDataField(hourTime)
        out.collect(userBrowser)
    }
    //天
    firstDay match {
      case true =>
        userBrowser.setOldCount(1L)
        userBrowser.setDataField(dayTime)
        out.collect(userBrowser)
      case _ =>
        userBrowser.setOldCount(0L)
        userBrowser.setDataField(dayTime)
        out.collect(userBrowser)
    }
    //月
    firstMonth match {
      case true =>
        userBrowser.setOldCount(1L)
        userBrowser.setDataField(monthTime)
        out.collect(userBrowser)
      case _ =>
        userBrowser.setOldCount(0L)
        userBrowser.setDataField(monthTime)
        out.collect(userBrowser)
    }

  }
}
