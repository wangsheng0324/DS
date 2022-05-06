package com.wscode.reduce

import com.wscode.bean.ChannelRegion
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * Created by angel
  */
class ChannelRegionReduce extends ReduceFunction[ChannelRegion]{
  override def reduce(before: ChannelRegion, after: ChannelRegion): ChannelRegion = {
    val aggregateField = before.getAggregateField
    val dataField = before.getDataField
    val timeStamp = before.getTimeStamp
    val channelID = before.getChannelID
    val country = before.getCountry
    val province = before.getProvince
    val city = before.getCity

    val pv = before.getPV + after.getPV
    val uv = before.getUV + after.getUV

    val newCount = before.getNewCount + after.getNewCount
    val oldCount = before.getOldCount + after.getOldCount

    val channelRegion = new ChannelRegion
    channelRegion.setAggreagateField(aggregateField)
    channelRegion.setChannelID(channelID)
    channelRegion.setDataField(dataField)
    channelRegion.setTimeStamp(timeStamp)
    channelRegion.setCity(city)
    channelRegion.setCountry(country)
    channelRegion.setProvince(province)
    channelRegion.setPV(pv)
    channelRegion.setUV(uv)
    channelRegion.setNewCount(newCount)
    channelRegion.setOldCount(oldCount)

    channelRegion
  }
}
