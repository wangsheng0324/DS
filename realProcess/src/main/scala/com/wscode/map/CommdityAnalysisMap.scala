package com.wscode.map

import com.wscode.bean.{CommodityAnalysis, OrderRecord}
import com.wscode.tools.TimeUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/**
 * @title: CommdityAnalysisMap
 * @projectName DS
 * @description: TODO
 * @author wangSheng
 * @date 2022/4/25 18:43
 */
class CommdityAnalysisMap extends FlatMapFunction[OrderRecord, CommodityAnalysis] {
  override def flatMap(orderRecord: OrderRecord, out: Collector[CommodityAnalysis]): Unit = {
    val commodityId = orderRecord.getCommodityId
    val createTime = orderRecord.getCreateTime
    val time = TimeUtils.formatYYYYmmdd(createTime).getOrElse("0000")
    val payTime = orderRecord.getPayTime
    var transactionCount: Long = 0L
    var unTransactionCount: Long = 0L
    if (StringUtils.isNotBlank(payTime)) {
      transactionCount = 1L
    } else {
      unTransactionCount = 1L
    }
    val commodityAnalysis = new CommodityAnalysis
    commodityAnalysis.setCommodityId(commodityId)
    commodityAnalysis.setTime(time)
    commodityAnalysis.setTransactionCount(transactionCount)
    commodityAnalysis.setUnTransactionCount(unTransactionCount)
    commodityAnalysis.setAggregateField(time + commodityId)

    out.collect(commodityAnalysis)
  }

}
