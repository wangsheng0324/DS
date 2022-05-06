package com.wscode.reduce

import com.wscode.bean.UserBrowser
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * Created by angel
  */
class UserBrowserReduce extends ReduceFunction[UserBrowser]{
  override def reduce(before: UserBrowser, after: UserBrowser): UserBrowser = {
    val dataField = before.getDataField
    val browser = before.getBrowser
    val tiemStamp = before.getTimeStamp

    val count = before.getCount + after.getCount
    val newCount = before.getNewCount + after.getNewCount
    val oldCount = before.getOldCount + after.getOldCount

    val userBrowser = new UserBrowser
    userBrowser.setDataField(dataField)
    userBrowser.setOldCount(oldCount)
    userBrowser.setNewCount(newCount)
    userBrowser.setCount(count)
    userBrowser.setBrowser(browser)
    userBrowser.setTimeStamp(tiemStamp)
    userBrowser
  }
}
