package com.wscode.map

import com.wscode.bean.{Message, UserNetwork, UserState}
import com.wscode.tools.TimeUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * Created by angel
 */
class UserNetworkMap extends FlatMapFunction[Message, UserNetwork] {
  val hour: String = "yyyyMMddhh"
  val day: String = "yyyyMMdd"
  val month: String = "yyyyMM"

  override def flatMap(value: Message, out: Collector[UserNetwork]): Unit = {
    val timeStamp = value.timeStamp
    val hourTime = TimeUtils.getData(timeStamp, hour)
    val dayTime = TimeUtils.getData(timeStamp, day)
    val monthTime = TimeUtils.getData(timeStamp, month)

    val userScan = value.userScan
    val userID = userScan.userID
    val network = userScan.network
    //根据用户id和时间查看用户的上网状态
    val userState = UserState.getUserState(userID, timeStamp)
    val isNew = userState.isNew
    val firstHour = userState.isFirstHour
    val firstDay = userState.isFirstDay
    val firstMonth = userState.isFirstMonth

    val userNetwork = new UserNetwork
    userNetwork.setNetwork(network)
    userNetwork.setTimeStamp(timeStamp)
    userNetwork.setCount(1L)

    //新用户
    isNew match {
      case true => userNetwork.setNewCount(1L)
      case _ => userNetwork.setNewCount(0L)
    }

    //小时
    firstHour match {
      case true =>
        userNetwork.setOldCount(1L)
        userNetwork.setDataField(hourTime)
        out.collect(userNetwork)
      case _ =>
        userNetwork.setOldCount(0L)
        userNetwork.setDataField(hourTime)
        out.collect(userNetwork)
    }
    //天
    firstDay match {
      case true =>
        userNetwork.setOldCount(1L)
        userNetwork.setDataField(dayTime)
        out.collect(userNetwork)
      case _ =>
        userNetwork.setOldCount(0L)
        userNetwork.setDataField(dayTime)
        out.collect(userNetwork)
    }
    //月
    firstMonth match {
      case true =>
        userNetwork.setOldCount(1L)
        userNetwork.setDataField(monthTime)
        out.collect(userNetwork)
      case _ =>
        userNetwork.setOldCount(0L)
        userNetwork.setDataField(monthTime)
        out.collect(userNetwork)
    }
  }
}
