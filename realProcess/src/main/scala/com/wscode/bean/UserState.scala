package com.wscode.bean

import com.wscode.tools.{HbaseUtils, TimeUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName

/**
 * 用户的访问状态
 */
case class UserState(
                      //是否是新用户
                      var isNew: Boolean = false,
                      //是否是小时段内的第一次来访
                      var isFirstHour: Boolean = false,
                      //是否是天段内的第一次来访
                      var isFirstDay: Boolean = false,
                      var isFirstMonth: Boolean = false
                    )

object UserState {
  def getUserState(userID: String, timeStamp: Long): UserState = {
    val tableName = TableName.valueOf("UserState")
    val rowkey = userID
    val columnFamily = "info"
    val firstVisitTimeColumn = "firstVisitTime"
    val lastVisitTimeColumn = "lastVisitTime"

    //是否是新用户
    var isNew: Boolean = false
    //是否是小时段内的第一次来访
    var isFirstHour: Boolean = false
    //是否是天段内的第一次来访
    var isFirstDay: Boolean = false
    var isFirstMonth: Boolean = false

    //去Hbase中查询UserState ， 查看是否是新用户：rowkey = userID
    val data = HbaseUtils.getData(tableName, rowkey, columnFamily, firstVisitTimeColumn)
    //如果data为null说明是新用户 ----> 那么直接赋值UserState,将新用户的数据插入到habse里面

    if (StringUtils.isBlank(data)) { //新用户

      try {
        var map = Map[String, Long]()
        map += (firstVisitTimeColumn -> timeStamp)
        map += (lastVisitTimeColumn -> timeStamp)
        HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
      } catch {
        case e: Exception => e.printStackTrace()
      }
      //是否是新用户
      isNew = true
      //是否是小时段内的第一次来访
      isFirstHour = true
      //是否是天段内的第一次来访
      isFirstDay = true
      isFirstMonth = true
      UserState(isNew, isFirstHour, isFirstDay, isFirstMonth)
    } else {
      try {
        //如果data不为null不是新用户 ----> 对比时间 ， Hbase表（UserState）的时间 < 日志过来的时间，这样才算是用户的新访问日志
        val lastVistData = HbaseUtils.getData(tableName, rowkey, columnFamily, lastVisitTimeColumn)
        if (StringUtils.isNotBlank(lastVistData)) {
          val date = lastVistData.toLong
          //小时
          if (TimeUtils.getData(timeStamp, "yyyyMMddhh").toLong >= date) {
            isFirstHour = true
          }
          //天
          if (TimeUtils.getData(timeStamp, "yyyyMMdd").toLong >= date) {
            isFirstDay = true
          }
          //月
          if (TimeUtils.getData(timeStamp, "yyyyMM").toLong >= date) {
            isFirstMonth = true
          }

        }
        HbaseUtils.putData(tableName, rowkey, columnFamily, lastVisitTimeColumn, timeStamp.toString)
      } catch {
        case e: Exception => e.printStackTrace()
      }
      UserState(isNew, isFirstHour, isFirstDay, isFirstMonth)
    }

  }
}
