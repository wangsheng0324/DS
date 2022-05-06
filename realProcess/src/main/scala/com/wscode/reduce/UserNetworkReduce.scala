package com.wscode.reduce

import com.wscode.bean.UserNetwork
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * Created by angel
  */
class UserNetworkReduce extends ReduceFunction[UserNetwork]{
  override def reduce(before: UserNetwork, after: UserNetwork): UserNetwork = {
    val dataField = before.getDataField
    val network = before.getNetwork
    val timeStamp = before.getTimeStamp
    val count = before.getCount + after.getCount
    val newCount = before.getNewCount + after.getNewCount
    val oldCount = before.getOldCount + after.getOldCount

    val userNetwork = new UserNetwork
    userNetwork.setDataField(dataField)
    userNetwork.setOldCount(oldCount)//用户上网方式的老用户数量
    userNetwork.setNewCount(newCount)//用户上网方式的新用户数量
    userNetwork.setTimeStamp(timeStamp)
    userNetwork.setNetwork(network)
    userNetwork.setCount(count)//用户上网方式的总数量
    userNetwork
  }
}
