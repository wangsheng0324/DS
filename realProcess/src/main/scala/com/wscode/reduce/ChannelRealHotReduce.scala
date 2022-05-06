package com.wscode.reduce

import com.wscode.bean.ChannelRealHot
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * Created by angel
  */
class ChannelRealHotReduce extends ReduceFunction[ChannelRealHot]{
  override def reduce(v1: ChannelRealHot, v2: ChannelRealHot): ChannelRealHot = {
    val channelID = v1.getChannelID
    val count = v1.getCount + v2.getCount
    val channelRealHot = new ChannelRealHot
    channelRealHot.setChannelID(channelID)
    channelRealHot.setCount(count)
    channelRealHot
  }
}
