package com.wscode.sink

import com.wscode.bean.UserNetwork
import com.wscode.tools.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.hbase.TableName

/**
 * Created by angel
 */
class UserNetworkSink extends SinkFunction[UserNetwork] {
  override def invoke(value: UserNetwork): Unit = {
    val network = value.getNetwork
    val timeStamp = value.getTimeStamp
    var count = value.getCount //每秒钟的用户上网方式总数量
    var newCount = value.getNewCount
    var oldCount = value.getOldCount

    val tableName = TableName.valueOf("device")
    val columnFamily = "info"
    val networkCountColumn = "networkCount"
    val networkNewCountColumn = "networkNewCount"
    val networkOldCountColumn = "networkOldCount"
    val rowkey = value.getDataField + "_" + network
    //查询历史数据，如果历史数据存在，那么叠加
    val networkData = HbaseUtils.getData(tableName, rowkey, columnFamily, networkCountColumn)
    val networkNewCount = HbaseUtils.getData(tableName, rowkey, columnFamily, networkNewCountColumn)
    val networkOldCount = HbaseUtils.getData(tableName, rowkey, columnFamily, networkOldCountColumn)

    if (StringUtils.isNotBlank(networkData)) {
      count = count + networkData.toLong
    }
    if (StringUtils.isNotBlank(networkNewCount)) {
      newCount = newCount + networkNewCount.toLong
    }
    if (StringUtils.isNotBlank(networkOldCount)) {
      oldCount = oldCount + networkOldCount.toLong
    }

    var map = Map[String, Long]()
    map += (networkCountColumn -> count)
    map += (networkNewCountColumn -> newCount)
    map += (networkOldCountColumn -> oldCount)

    HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)
  }
}
