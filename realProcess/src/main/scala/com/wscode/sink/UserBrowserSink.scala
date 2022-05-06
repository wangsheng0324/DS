package com.wscode.sink

import com.wscode.bean.UserBrowser
import com.wscode.tools.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.hbase.TableName

/**
 * Created by angel
 */
class UserBrowserSink extends SinkFunction[UserBrowser] {
  override def invoke(value: UserBrowser): Unit = {
    val tableName = TableName.valueOf("device")
    val columnFamily = "info"
    val columnCount = "userBrowserCount"
    val columnNewCount = "userBrowserNewCount"
    val columnOldCount = "userBrowserOldCount"
    val timeStamp = value.getTimeStamp
    val rowkey = value.getDataField + ":" + value.getBrowser

    //查询历史数据，如果历史数据存在，那么叠加
    val count = HbaseUtils.getData(tableName, rowkey, columnFamily, columnCount)
    val newCount = HbaseUtils.getData(tableName, rowkey, columnFamily, columnNewCount)
    val oldCount = HbaseUtils.getData(tableName, rowkey, columnFamily, columnOldCount)

    var totalCount = value.getCount
    var newCount1 = value.getNewCount
    var oldCount1 = value.getOldCount

    if (StringUtils.isNotBlank(count)) {
      totalCount = totalCount + count.toLong
    }
    if (StringUtils.isNotBlank(newCount)) {
      newCount1 = newCount1 + newCount.toLong
    }
    if (StringUtils.isNotBlank(oldCount)) {
      oldCount1 = oldCount1 + oldCount.toLong
    }

    //构造数据
    var map = Map[String, Long]()
    map += (columnCount -> totalCount)
    map += (columnNewCount -> newCount1)
    map += (columnOldCount -> oldCount1)

    //数据落地
    HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
  }
}
