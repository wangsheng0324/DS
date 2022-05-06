package com.wscode.task

import com.wscode.bean.{CommodityAnalysis, OrderRecord}
import com.wscode.map.{CommdityAnalysisMap, HbaseToOrderRecordMap}
import com.wscode.reduce.CommdityAnalysisReduce
import com.wscode.tools.{GlobalConfigUtils, HbaseUtils}
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}

/**
 * @title: CommodityAnalysisTask
 * @projectName DS
 * @description: TODO 批量 - 商品交易分析
 * @author wangSheng
 * @date 2022/4/22 18:30
 */
object CommodityAnalysisTask {
  //Hbase连接
  val tableName: TableName = TableName.valueOf("orderRecord")
  var conn: Connection = null

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val source: DataSet[Tuple2[String, String]] = env.createInput(new TableInputFormat[Tuple2[String, String]] {
      // 构建rowkey
      override def mapResultToTuple(result: Result): Tuple2[String, String] = {
        val rowkey = Bytes.toString(result.getRow)
        val sb = new StringBuffer()
        for (cell: Cell <- result.rawCells()) {
          val value = Bytes.toString(cell.getValueArray,
            cell.getValueOffset, cell.getValueLength)
          sb.append(value).append(",")
        }
        val valueString = sb.replace(sb.length() - 1, sb.length(), "").toString
        val tuple2 = new Tuple2[String, String]
        tuple2.setField(rowkey, 0)
        tuple2.setField(valueString, 1)
        tuple2
      }

      override def getTableName: String = "orderRecord"

      override def getScanner: Scan = {
        val conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, GlobalConfigUtils.hbaseQuorem)
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, GlobalConfigUtils.clientPort)
        // 构建连接
        conn = ConnectionFactory.createConnection(conf)
        table = classOf[HTable].cast(conn.getTable(tableName))
        scan = new Scan() {
          addFamily(Bytes.toBytes("info"))
        }
        scan
      }

      override def close() = {
        if (table != null) {
          table.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })

    //将hbase中的订单表转化成订单实体类
    val orderMap: DataSet[OrderRecord] = source.map(new HbaseToOrderRecordMap)
    //根据订单得到交易数据
    val commodityAnalysisMap: DataSet[CommodityAnalysis] = orderMap.flatMap(new CommdityAnalysisMap)
    //对成交数据中的按照time+commodityId 进⾏叠加
    val groupeCommodityAnalysis: GroupedDataSet[CommodityAnalysis] = commodityAnalysisMap.groupBy(line => line.getAggregateField)
    val reduceCommodityAnalysis: DataSet[CommodityAnalysis] = groupeCommodityAnalysis.reduce(new CommdityAnalysisReduce)
    //交易数据落地
    val resultData: Seq[CommodityAnalysis] = reduceCommodityAnalysis.collect()
    for (value <- resultData) {
      val commodityId = value.getCommodityId
      val time = value.getTime
      val transactionCount = value.getTransactionCount
      val unTransactionCount = value.getUnTransactionCount
      var map = Map[String, Long]()
      map += ("transactionCount" -> transactionCount)
      map += ("unTransactionCount" -> unTransactionCount)
      HbaseUtils.putMapData(TableName.valueOf("CommdityAnalysis"), time + ":" + commodityId, "info", map)
    }


  }
}
