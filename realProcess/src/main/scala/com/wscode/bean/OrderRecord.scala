package com.wscode.bean

/**
 * @title: OrderRecord
 * @projectName DS
 * @description: TODO 订单记录类
 * @author wangSheng
 * @date 2022/4/25 18:04
 */
class OrderRecord {

  private var orderId: Int = 0 // 订单ID
  private var userId: Int = 0 // 用户ID
  private var merchantId: Int = 0 // 商户ID
  private var orderAmount: Double = 0.0 // 订单金额
  private var payAmount: Double = 0.0 // 支付金额
  private var payMethod: Int = 0 // 支付方式
  private var payTime: String = "" // 支付时间
  private var benefitAmount: Double = 0.0 // 优惠金额
  private var voucherAmount: Double = 0.0 // 券金额
  private var commodityId: Int = 0 // 商品ID
  private var activityNum: Int = 0 // 活动编号
  private var createTime: String = "" // 创建时间

  def getOrderId: Int = orderId
  def getUserId: Int = userId
  def getMerchantId: Int = merchantId
  def getOrderAmount: Double = orderAmount
  def getPayAmount: Double = payAmount
  def getPayMethod: Int = payMethod
  def getPayTime: String = payTime
  def getBenefitAmount: Double = benefitAmount
  def getVoucherAmount: Double = voucherAmount
  def getCommodityId: Int = commodityId
  def getActivityNum: Int = activityNum
  def getCreateTime: String = createTime

  def setOrderId(orderId: Int): Unit = {this.orderId = orderId}
  def setUserId(userId: Int): Unit = {this.userId = userId}
  def setMerchantId(merchantId: Int): Unit = {this.merchantId = merchantId}
  def setOrderAmount(orderAmount: Double): Unit = {this.orderAmount = orderAmount}
  def setPayAmount(payAmount: Double): Unit = {this.payAmount = payAmount}
  def setPayMethod(payMethod: Int): Unit = {this.payMethod = payMethod}
  def setPayTime(payTime: String): Unit = {this.payTime = payTime}
  def setBenefitAmount(benefitAmount: Double): Unit = {this.benefitAmount = benefitAmount}
  def setVoucherAmount(voucherAmount: Double): Unit = {this.voucherAmount = voucherAmount}
  def setCommodityId(commodityId: Int): Unit = {this.commodityId = commodityId}
  def setActivityNum(activityNum: Int): Unit = {this.activityNum = activityNum}
  def setCreateTime(createTime: String): Unit = {this.createTime = createTime}


}
