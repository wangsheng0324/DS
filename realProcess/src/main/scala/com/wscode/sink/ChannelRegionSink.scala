package com.wscode.sink

import com.wscode.bean.ChannelRegion
import com.wscode.tools.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.hbase.TableName

/**
 * Created by angel
 */
class ChannelRegionSink extends SinkFunction[ChannelRegion] {
  override def invoke(value: ChannelRegion): Unit = {
    val tableName = TableName.valueOf("channel")
    val columnFamily = "info"
    val regionPVColumn = "regionPV"
    val regionUVColumn = "regionUV"
    val regionNewCountColumn = "regionNewCount"
    val regionOldCountColumn = "regionOldCount"
    val rowkey = value.getChannelID + ":" + value.getDataField + ":" + value.getCountry + value.getProvince + value.getCity
    var pv = value.getPV
    var uv = value.getUV
    var newCount = value.getNewCount
    var oldCount = value.getOldCount

    //去Hbase表(channel)中查询 , 是否有地域相关的PVUV和新鲜度数据，如果有需要叠加操作
    val pvData = HbaseUtils.getData(tableName, rowkey, columnFamily, regionPVColumn)
    val uvData = HbaseUtils.getData(tableName, rowkey, columnFamily, regionUVColumn)
    val newCountData = HbaseUtils.getData(tableName, rowkey, columnFamily, regionNewCountColumn)
    val oldCountData = HbaseUtils.getData(tableName, rowkey, columnFamily, regionOldCountColumn)
    pvData match {
      case x => if (StringUtils.isNotBlank(x)) {
        pv = pv + pvData.toLong
      }
    }
    uvData match {
      case x => if (StringUtils.isNotBlank(x)) {
        uv = uv + uvData.toLong
      }
    }
    newCountData match {
      case x => if (StringUtils.isNotBlank(x)) {
        newCount = newCount + newCountData.toLong
      }
    }
    oldCountData match {
      case x => if (StringUtils.isNotBlank(x)) {
        oldCount = oldCount + oldCountData.toLong
      }
    }

    var map = Map[String, Long]()
    map += (regionPVColumn -> pv)
    map += (regionUVColumn -> uv)
    map += (regionNewCountColumn -> newCount)
    map += (regionOldCountColumn -> oldCount)

    HbaseUtils.putMapData(tableName, rowkey, columnFamily, map)

  }
}
