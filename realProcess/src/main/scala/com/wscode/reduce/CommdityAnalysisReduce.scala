package com.wscode.reduce

import com.wscode.bean.CommodityAnalysis
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * @title: CommdityAnalysisReduce
 * @projectName DS
 * @description: TODO
 * @author wangSheng
 * @date 2022/4/25 18:56
 */
class CommdityAnalysisReduce extends ReduceFunction[CommodityAnalysis] {
  override def reduce(v1: CommodityAnalysis, v2: CommodityAnalysis): CommodityAnalysis = {
    val time = v1.getTime
    val commodityId = v1.getCommodityId
    val transactionCount = v1.getTransactionCount + v2.getTransactionCount
    val unTransactionCount = v1.getUnTransactionCount + v2.getUnTransactionCount
    val commodityAnalysis = new CommodityAnalysis
    commodityAnalysis.setCommodityId(commodityId)
    commodityAnalysis.setTime(time)
    commodityAnalysis.setTransactionCount(transactionCount)
    commodityAnalysis.setUnTransactionCount(unTransactionCount)
    commodityAnalysis

  }
}
