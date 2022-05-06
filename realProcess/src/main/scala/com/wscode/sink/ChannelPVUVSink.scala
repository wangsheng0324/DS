package com.wscode.sink

import com.wscode.bean.ChannelPVUV
import com.wscode.tools.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.hbase.TableName

/**
  * Created by angel
  */
class ChannelPVUVSink extends SinkFunction[ChannelPVUV]{
  override def invoke(value: ChannelPVUV): Unit = {
    val tableName = TableName.valueOf("channel")
    val columnFamily = "info"

    var pV = value.getPV
    var uV = value.getUV
    val rowkey = value.getChannelID+":"+value.getDateField
    //落地到hbase的channel这个表，以前也有PVUV数据
    //1:将历史数据查询出来，然后和现在的数据做一次叠加操作
    //2：在落地到hbase中
    val pvData = HbaseUtils.getData(tableName , rowkey , columnFamily , "pv")
    val uvData = HbaseUtils.getData(tableName , rowkey , columnFamily , "uv")
    if(StringUtils.isNotBlank(pvData)){
      pV = pV + pvData.toLong
    }
    if(StringUtils.isNotBlank(uvData)){
      uV = uV + uvData.toLong
    }
    var map = Map[String , Long]()
    map += ("pv" -> pV)
    map += ("uv" -> uV)
    //将最终结果落地到hbase
    HbaseUtils.putMapData(tableName , rowkey , columnFamily , map)

  }
}
