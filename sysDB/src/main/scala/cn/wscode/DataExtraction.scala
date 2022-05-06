package cn.wscode

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import java.util
import java.util.Properties

/**
 * @title: DataExtraction
 * @projectName DS
 * @description: TODO
 * @author wangSheng
 * @date 2022/4/22 16:20
 */
object DataExtraction {
  //  1：指定相关信息（flink对接kafka ， flink对接Hbase）

  //  2：创建流处理环境

  //  3：创建kafka的数据流

  //  4：添加数据源addSource(kafka09)

  //  5：解析kafka数据流 ， 封装成Canal

  //  6：数据落地
  //指定相关信息（flink对接kafka ， flink对接Hbase）
  val zkCluster = "hadoop01,hadoop02,hadoop03"
  val kafkaCluster = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
  val kafkaTopicName = "canal"
  val hbasePort = "2181"
  val columnFamily = "info"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink-checkpoint/checkpoint/"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //处理时间时间
    env.getConfig.setAutoWatermarkInterval(2000) //水印间隔
    env.getCheckpointConfig.setCheckpointInterval(5000) //每隔多少秒进行checkpoint
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    System.setProperty("hadoop.home.dir", "/") // mac环境下需要设置
    //创建kafka的数据流
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaCluster)
    properties.setProperty("zookeeper.connect", zkCluster)
    properties.getProperty("group.id", "canal")
    val kafka09: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](kafkaTopicName, new SimpleStringSchema(), properties)
    //添加数据源addSource(kafka09)
    val source: DataStream[String] = env.addSource(kafka09)

    //解析kafka数据流 ， 封装成Canal
    val mapData: DataStream[Canal] = source.map {
      line =>
        val values = line.split("#CS#")
        val valuesLength = values.length
        val fileName = if (valuesLength > 0) values(0) else ""
        val fileOffset = if (valuesLength > 1) values(1) else ""
        val dbName = if (valuesLength > 2) values(2) else ""
        val tableName = if (valuesLength > 3) values(3) else ""
        //logFileName + "#CS#" + logFileOffset + "#CS#" + dbName + "#CS#" + tableName + "#CS#"
        //+eventType + "#CS#" + list1 + emptyCount;
        val eventType = if (valuesLength > 4) values(4) else ""
        val columns = if (valuesLength > 5) values(5) else ""
        val rowNum = if (valuesLength > 6) values(6) else ""
        Canal(fileName, fileOffset, dbName, tableName, eventType, columns, rowNum)
    }
    mapData.print()

    //6：数据落地
    //Canal(mysql-bin.000001,3254,PYG,commodity,INSERT,[[commodityId, 6, true], [commodityName, 欧派, true], [commodityTypeId, 3, true], [originalPrice, 43000.0, true], [activityPrice, 40000.0, true]]7,)
    mapData.map(
      line => {

        val str: String = line.columns

        //处理掉不需要的字符串
        val triggerFields = substrCanal(str)
        //获取（变更的）触发规则的列
        val eventType = line.eventType
        //发生改变的列---》true
        val columns: util.ArrayList[UpdateFields] = getTriggerColumns(triggerFields, eventType)

        val tableName = line.tableName //mysql叫什么表，hbase就叫什么表
        if (eventType.equals("DELETE")) { //同步删除操作
          tableName match {
            case "shopCartAnalysis" =>
              val rowkey = getShopCartAnalysis(triggerFields)
              operatorHbaseDelete(tableName, rowkey, eventType)
            case _ =>
              //获取hbase的rwokey
              val rowkey = line.dbName + "_" + line.tableName + ":" + getPrimaryKey(line.columns)
              //数据库同步操作了
              operatorHbaseDelete(tableName, rowkey, eventType)
          }

        } else { //更新操作
          if (columns.size() > 0) {
            tableName match {
              case "shopCartAnalysis" =>
                val rowkey = getShopCartAnalysis(triggerFields)
                operatorHbase(tableName, rowkey, eventType, columns)
              case _ =>
                val rowkey = line.dbName + "_" + line.tableName + ":" + getPrimaryKey(line.columns)
                //数据库同步操作了
                operatorHbase(tableName, rowkey, eventType, columns)
            }
          }
        }

      }
    )
    env.execute("DataExtract")


  }

  //获取购物车分析的rowkey=添加时间+用户ID+产品ID+商家ID
  def getShopCartAnalysis(triggerFields: String): String = {
    val arrays: Array[String] = StringUtils.substringsBetween(triggerFields, "[", "]")
    val userid = arrays(0).split(",")(1).trim
    val commodityId = arrays(1).split(",")(1).trim
    val commodityNum = arrays(2).split(",")(1).trim
    val commodityAmount = arrays(3).split(",")(1).trim
    val addTime = arrays(4).split(",")(1).trim
    val merchantId = arrays(5).split(",")(1).trim
    val rowkey = addTime + userid + commodityId + merchantId
    rowkey
  }

  ////[commodityId, 6, true], [commodityName, 欧派, true], [commodityTypeId, 3, true], [originalPrice, 43000.0, true], [activityPrice, 40000.0, true]
  def getPrimaryKey(columns: String): String = {
    val arrays = StringUtils.substringsBetween(columns, "[", "]")
    val primaryStr = arrays(0) //[commodityId, 6, true]
    primaryStr.split(",")(1).trim

  }


  def substrCanal(line: String): String = {
    line.substring(1, line.length - 1)
  }

  def getTriggerColumns(columns: String, eventType: String): util.ArrayList[UpdateFields] = {
    //将字符串转换成数组
    val arrays: Array[String] = StringUtils.substringsBetween(columns, "[", "]")
    val list = new util.ArrayList[UpdateFields]()
    eventType match {
      case "UPDATE" =>
        for (index <- 0 to arrays.length - 1) {
          val split: Array[String] = arrays(index).split(",")
          if (split(2).trim.toBoolean == true) {
            list.add(UpdateFields(split(0), split(1)))
          }
        }
        list
      case "INSERT" =>
        for (index <- 0 to arrays.length - 1) {
          val split: Array[String] = arrays(index).split(",")
          list.add(UpdateFields(split(0), split(1)))
        }
        list
      case _ =>
        list
    }
  }


  //更新hbase操作
  def operatorHbase(tableName: String, rowkey: String, eventType: String, columns: util.ArrayList[UpdateFields]) = {
    //1:指定相关参数
    //1: 封装hbase的参数
    val config: Configuration = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", zkCluster)
    config.set("hbase.master", "hadoop01:60000")
    config.set("hbase.zookeeper.property.clientPort", hbasePort)
    config.setInt("hbase.rpc.timeout", 60000)
    config.setInt("hbase.client.operator.timeout", 60000)
    //def scannTimeout = conf.getString("c")
    config.setInt("hbase.client.scanner.timeout.period", 60000)
    //2:构建hbase的连接操作
    val conn: Connection = ConnectionFactory.createConnection(config)
    //3:获取hbase的客户端操作
    val admin: Admin = conn.getAdmin
    //1):构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    //2）：构建列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)
    hTableDescriptor.addFamily(hColumnDescriptor)
    //3）：如果表不存在则创建表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      //如果不存在，那么创建表
      admin.createTable(hTableDescriptor)
    }


    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      //rowkey --->put
      val put = new Put(Bytes.toBytes(rowkey))
      for (index <- 0 to columns.size() - 1) {
        val fields = columns.get(index)
        val keys = fields.keys //就相当于Hbase表的列
        val value = fields.value //相当于hbase的列的值
        //将值添加到habse的插入条件中
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(keys.toString), Bytes.toBytes(value.toString))
      }
      table.put(put)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
      conn.close()
    }
  }

  //hbase删除
  def operatorHbaseDelete(tableName: String, rowkey: String, eventType: String) = {
    //1:指定相关参数
    //1: 封装hbase的参数
    val config: Configuration = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", zkCluster)
    config.set("hbase.master", "hadoop01:60000")
    config.set("hbase.zookeeper.property.clientPort", hbasePort)
    config.setInt("hbase.rpc.timeout", 60000)
    config.setInt("hbase.client.operator.timeout", 60000)
    //def scannTimeout = conf.getString("c")
    config.setInt("hbase.client.scanner.timeout.period", 60000)
    //2:构建hbase的连接操作
    val conn: Connection = ConnectionFactory.createConnection(config)
    //3:获取hbase的客户端操作
    val admin: Admin = conn.getAdmin
    //1):构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    //2）：构建列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)
    hTableDescriptor.addFamily(hColumnDescriptor)
    //3）：如果表不存在则创建表
    if (!admin.tableExists(TableName.valueOf(tableName))) {
      //如果不存在，那么创建表
      admin.createTable(hTableDescriptor)
    }


    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val delete = new Delete(Bytes.toBytes(rowkey))
      table.delete(delete)

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      table.close()
      conn.close()
    }
  }

}

case class Canal(
                  fileName: String,
                  fileOffset: String,
                  dbName: String,
                  tableName: String,
                  eventType: String,
                  columns: String,
                  rowNum: String
                )

//只保存：触发的列（true） keys:列名称  value：列的值
case class UpdateFields(keys: String, value: String)
