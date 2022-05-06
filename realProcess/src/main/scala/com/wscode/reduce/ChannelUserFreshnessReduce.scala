package com.wscode.reduce

import com.wscode.bean.ChannelUserFreshness
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * Created by angel
  */
class ChannelUserFreshnessReduce extends ReduceFunction[ChannelUserFreshness]{
  override def reduce(v1: ChannelUserFreshness, v2: ChannelUserFreshness): ChannelUserFreshness = {
    val aggregateField = v1.getAggregateField
    val dataField = v1.getDataField
    val timeStamp = v1.getTimeStamp
    val channelID = v1.getChannelID

    val newCount = v1.getNewCount + v2.getNewCount
    val oldCount = v1.getOldCount + v2.getOldCount

    //将数据结果封装到用户新鲜度类中
    val channelUserFreshness = new ChannelUserFreshness
    channelUserFreshness.setAggregateField(aggregateField)
    channelUserFreshness.setDataField(dataField)
    channelUserFreshness.setChannelID(channelID)
    channelUserFreshness.setNewCount(newCount)
    channelUserFreshness.setOldCount(oldCount)
    channelUserFreshness.setTimeStamp(timeStamp)

    channelUserFreshness
  }
}
