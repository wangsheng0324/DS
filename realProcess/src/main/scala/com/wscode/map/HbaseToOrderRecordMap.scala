package com.wscode.map

import com.wscode.bean.OrderRecord
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.Tuple2

/**
 * @title: HbaseToOrderRecord
 * @projectName DS
 * @description: TODO
 * @author wangSheng
 * @date 2022/4/25 18:03
 */
class HbaseToOrderRecordMap  extends MapFunction[Tuple2[String, String] , OrderRecord]  {
  override def map(value: Tuple2[String, String]): OrderRecord = {
    val arr = value.f1.split(",")
    val activityNum = arr(0).trim.toInt
    val benefitAmount = arr(1).trim.toDouble
    val commodityId = arr(2).trim.toInt
    val createTime = arr(3).trim
    val merchantId = arr(4).trim.toInt
    val orderAmount = arr(5).trim.toDouble
    val orderId = arr(6).trim.toInt
    val payAmount = arr(7).trim.toDouble
    val payMethod = arr(8).trim.toInt
    val payTime = arr(9).trim
    val userId = arr(10).trim.toInt
    val voucherAmount = arr(11).trim.toDouble
    val order = new OrderRecord
    order.setActivityNum(activityNum)
    order.setBenefitAmount(benefitAmount)
    order.setCommodityId(commodityId)
    order.setCreateTime(createTime)
    order.setMerchantId(merchantId)
    order.setOrderAmount(orderAmount)
    order.setOrderId(orderId)
    order.setPayAmount(payAmount)
    order.setPayMethod(payMethod)
    order.setPayTime(payTime)
    order.setUserId(userId)
    order.setVoucherAmount(voucherAmount)
    order
  }
}
