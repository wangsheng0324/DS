package com.wscode.sink

import com.wscode.bean.ChannelRealHot
import com.wscode.tools.HbaseUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.hbase.TableName

/**
  * Created by angel
  */
class ChannelRealHotSink extends SinkFunction[ChannelRealHot]{
  /**
    *1:制定好要插入的数据
    *2：去hbase中找到历史数据
    *3：如果历史数据不为空，那么进行merge
    *4：数据落地
    **/
  override def invoke(value: ChannelRealHot): Unit = {
    //制定好要插入的数据
    val channelID: Long = value.getChannelID.toLong
    var count: Long = value.getCount
    //去hbase中找到历史数据
    val tableName:TableName = TableName.valueOf("channel")
    val rowkey = value.getChannelID
    //如果历史数据不为空，那么进行merge
    val data: String = HbaseUtils.getData(tableName , rowkey , "info" , "count")
    if(StringUtils.isNotBlank(data)){
      count = count + data.toLong
    }
    var map = Map[String , Long]()
    map += ("count" -> count)
    //数据落地
    HbaseUtils.putMapData(tableName , rowkey , "info" , map)

  }
}
