package com.wscode.tools

import java.util.Date
import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.StringUtils

import java.util.concurrent.atomic.AtomicInteger

/**
  * Created by angel
  */
object TimeUtils {

  def getData(timeStamp:Long , format:String):String = {
    val time = new Date(timeStamp)
    // FastDateFormat 线程安全的
    val instance = FastDateFormat.getInstance(format)
    val format1 = instance.format(time)
    format1
  }

  def formatData(times:String): Option[String] = {
    //yyyy-MM-dd HH:mm:ss
    if(StringUtils.isNotBlank(times)){
      val fields: Array[String] = times.split(" ")
      if(fields.length > 1){
        Some(fields(0).replace("-", "") + fields(1).replace(":", ""))
      }else{
        None
      }
    }else{
      None
    }
  }
  def formatYYYYmmdd(times:String): Option[String] = {
    //yyyy-MM-dd HH:mm:ss
    if(StringUtils.isNotBlank(times)){
      val fields: Array[String] = times.split(" ")
      if(fields.length > 1){
        Some(fields(0).replace("-", ""))
      }else{
        None
      }
    }else{
      None
    }
  }
}
