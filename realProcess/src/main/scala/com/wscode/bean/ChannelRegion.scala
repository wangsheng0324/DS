package com.wscode.bean

/**
  * Created by angel
  */
class ChannelRegion {
  private var channelID:String = null
//  private var area:String = null
  private var country:String = null
  private var province:String = null
  private var city:String = null
  private var pv:Long = 0L
  private var uv:Long = 0L
  private var newCount:Long= 0L
  private var oldCount:Long = 0L
  private var timeStamp:Long = 0L
  private var dataField:String = null
  private var aggregateField:String = null


  def getChannelID = channelID
//  def getArea = area
  def getCountry = country
  def getProvince = province
  def getCity = city
  def getPV = pv
  def getUV = uv
  def getNewCount = newCount
  def getOldCount = oldCount
  def getTimeStamp = timeStamp
  def getDataField = dataField
  def getAggregateField = aggregateField


  def setChannelID(value:String) = {channelID = value}
//  def setArea(value:String) = {area = value}
  def setCountry(value:String) = {country = value}
  def setProvince(value:String) = {province = value}
  def setCity(value:String) = {city = value}
  def setPV(value:Long) = {pv = value}
  def setUV(value:Long) = {uv = value}
  def setNewCount(value:Long) = {newCount = value}
  def setOldCount(value:Long) = {oldCount = value}
  def setTimeStamp(value:Long) = {timeStamp = value}
  def setDataField(value:String) = {dataField = value}
  def setAggreagateField(value:String) = {aggregateField = value}


}
