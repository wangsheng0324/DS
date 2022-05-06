package com.wscode.bean

/**
 * @title: CommodityAnalysis
 * @projectName DS
 * @description: TODO
 * @author wangSheng
 * @date 2022/4/25 18:39
 */
class CommodityAnalysis {
  private var commodityId: Int = 0 //商品id
  private var time: String = "" //时间
  private var transactionCount: Long = 0 //交易次数
  private var unTransactionCount: Long = 0 //未交易次数
  private var aggregateField: String = "" //聚合字段

  def getCommodityId: Int = commodityId
  def setCommodityId(commodityId: Int): Unit = this.commodityId = commodityId
  def getTime: String = time
  def setTime(time: String): Unit = this.time = time
  def getTransactionCount: Long = transactionCount
  def setTransactionCount(transactionCount: Long): Unit = this.transactionCount = transactionCount
  def getUnTransactionCount: Long = unTransactionCount
  def setUnTransactionCount(unTransactionCount: Long): Unit = this.unTransactionCount = unTransactionCount
  def getAggregateField: String = aggregateField
  def setAggregateField(aggregateField: String): Unit = this.aggregateField = aggregateField


  override def toString = s"CommodityAnalysis($commodityId, $time, $transactionCount, $unTransactionCount, $aggregateField)"
}
