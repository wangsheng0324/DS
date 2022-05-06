package com.wscode.bean

/**
  * Created by angel
  */
class UserBrowser {
  private var browser:String = null
  private var count:Long = 0L
  private var newCount:Long = 0L
  private var oldCount:Long = 0L
  private var timeStamp:Long = 0L
  private var dataField:String = null

  def getBrowser = browser
  def getCount = count
  def getNewCount = newCount
  def getOldCount = oldCount
  def getTimeStamp = timeStamp
  def getDataField = dataField

  def setBrowser(value:String) = {browser = value}
  def setCount(value:Long) = {count = value}
  def setNewCount(value:Long) = {newCount = value}
  def setOldCount(value:Long) = {oldCount = value}
  def setTimeStamp(value:Long) = {timeStamp = value}
  def setDataField(value:String) = {dataField = value}

}
