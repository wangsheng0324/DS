package com.wscode.bean

/**
  * Created by angel
  */
class ChannelUserFreshness {
  private var channelID:String = null
  private var newCount:Long = 0L
  private var oldCount:Long = 0L
  private var timeStamp:Long  = 0L
  private var dataField:String = null
  private var aggregateField:String = null


  def getChannelID = channelID
  def getNewCount = newCount
  def getOldCount = oldCount
  def getTimeStamp = timeStamp
  def getDataField = dataField
  def getAggregateField = aggregateField

  def setChannelID(value:String) = {channelID = value}
  def setNewCount(value:Long) = {newCount = value}
  def setOldCount(value:Long) = {oldCount = value}
  def setTimeStamp(value:Long) = {timeStamp = value}
  def setDataField(value:String) = {dataField = value}
  def setAggregateField(value:String) = {aggregateField = value}
}
