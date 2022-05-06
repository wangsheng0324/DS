package com.wscode

import com.alibaba.fastjson.JSON
import com.wscode.bean.{Message, UserScan}
import com.wscode.task._
import com.wscode.tools.{GlobalConfigUtils, HbaseUtils, KafkaTools}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.hadoop.hbase.TableName

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

/**
 * todo 程序入口
 */
object App {
  def main(args: Array[String]): Unit = {

    // 添加可插拔的操作方式
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    if (parameterTool.getNumberOfParameters < 5) {
      println("Missing parameters! Usage: Kafka " +
        "--input-topic <topic> " +
        "--bootstrap.servers <kafka brokers> " +
        "--zookeeper.connect <zk quorum> " +
        "--group.id <some id>" +
        "--run <method>")
      return
    }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //处理进行checkpoint和水印
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //todo 参数生效
    env.getConfig.setGlobalJobParameters(parameterTool)

    //保证程序长时间运行的安全性进行checkpoint操作
    env.enableCheckpointing(5000) //checkpoint的时间间隔
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //checkpoint的模式
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000) //checkpoint最小的停顿间隔
    env.getCheckpointConfig.setCheckpointTimeout(60000) //checkpoint超时的时长
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) //允许的最大checkpoint并行度
    //当程序关闭的时候偶，会不会出发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink-checkpoint/"))
    System.setProperty("hadoop.home.dir", "/")

    //对接kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"))
    properties.setProperty("zookeeper.connect", parameterTool.getRequired("zookeeper.connect"))
    properties.setProperty("gruop.id", parameterTool.getRequired("gruop.id"))
    properties.setProperty("enable.auto.commit", GlobalConfigUtils.enableAutoCommit)
    properties.setProperty("auto.commit.interval.ms", GlobalConfigUtils.commitInterval)

    //下次重新消费的话，偶从哪里开始消费 latest：从上一次提交的offset位置开始的  earlist：从头开始进行
    properties.setProperty("auto.offset.reset", GlobalConfigUtils.offsetReset)
    //序列化
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      GlobalConfigUtils.inputTopic,
      new SimpleStringSchema(),
      properties
    )

    /**
     *
     * 如果checkpoint启⽤，当checkpoint完成之后，Flink Kafka Consumer将会提交offset保存
     * 到checkpoint State中，
     * 这就保证了kafka broker中的committed offset与 checkpoint stata中的offset相⼀致。
     * ⽤户可以在Consumer中调⽤setCommitOffsetsOnCheckpoints(boolean) ⽅法来选择启⽤
     * 或者禁⽤offset committing(默认情况下是启⽤的)
     * */
    consumer.setCommitOffsetsOnCheckpoints(true) // 启⽤提交offset
    consumer.setStartFromLatest() //从最新的开始消费
    consumer.setStartFromGroupOffsets() //从group的offset开始消费

    val source: DataStream[String] = env.addSource(consumer)
    //将kafka获取到的json数据解析封装成message类
    val message = source.map {
      line =>
        val value = JSON.parseObject(line)
        val count = value.get("count").toString.toInt
        val message = value.get("message").toString
        val timeStamp = value.get("timeStamp").toString.toLong
        val userScan: UserScan = UserScan.toBean(message)
        Message(userScan, count, timeStamp)
    }
    //添加flink的水印处理 , 允许得最大延迟时间是2S
    val watermarkData: DataStream[Message] = message.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {
      var currentTimestamp: Long = 0L
      val maxDelayTime = 2000L
      var watermark: Watermark = null

      //获取当前的水印
      override def getCurrentWatermark = {
        watermark = new Watermark(currentTimestamp - maxDelayTime)
        watermark
      }

      //时间戳抽取操作
      override def extractTimestamp(t: Message, l: Long) = {
        val timeStamp = t.timeStamp
        currentTimestamp = Math.max(timeStamp, currentTimestamp)
        currentTimestamp
      }
    })

    // 获取kafka的偏移量
    val offset_0: (Int, Long) = KafkaTools.getCommittedOffset(parameterTool.getRequired("input.topic"), 0)
    val offset_1: (Int, Long) = KafkaTools.getCommittedOffset(parameterTool.getRequired("input.topic"), 1)
    val offset_2: (Int, Long) = KafkaTools.getCommittedOffset(parameterTool.getRequired("input.topic"), 2)

    println("----------------------->", offset_0, offset_1, offset_2)
    val atomic: AtomicInteger = new AtomicInteger(0)
    var semaphore: Long = 0L //实时事务信号量

    /**
     * 确保当业务处理成功后，提交offset
     */
    try {
      val method: String = parameterTool.getRequired("run")
      method match {
        case "RealHots" => {
          //TODO 处理实时热点数据
          semaphore = ChannelRealHotTask.process(watermarkData, atomic)
          println("============RealHots===================")
        }
        case "PvUv" => {
          //TODO 处理频道的PV、UV总情况落地hbase
          semaphore = ChannelPVUVTask.process(watermarkData, atomic)
          println("============PvUv===================")
        }
        case "Freshness" => {
          //TODO 处理频道的新鲜度
          semaphore = ChannelUserFreshnessTask.process(watermarkData, atomic)
          println("============Freshness===================")
        }
        case "Area" => {
          //TODO 频道地域分布
          semaphore = ChannelRegionTask.process(watermarkData, atomic)
          println("============Area===================")
        }
        case "NetWork" => {
          //TODO 运营商平台
          semaphore = UserNetworkTask.process(watermarkData, atomic)
          println("============NetWork===================")
        }
        case "Brower" => {
          //TODO 浏览器类型
          semaphore = UserBrowserTask.process(watermarkData, atomic)
          println("============Brower===================")
        }
        case _ => {
          println("The parameter input error, you must enter the following parameters：" + "[RealHots、PvUv、Freshness、Area、NetWork、Brower]")
        }
      }
    } catch {
      case e: Exception =>
        val tableName = TableName.valueOf("ds_err")
        //TOPIC + partition + offset
        val rowkey = parameterTool.getRequired("input-topic") + offset_0._2 + offset_1._2 + offset_2._2
        //tableName: TableName , rowkey:String , columnFamily:String ,
        var map = Map[String, Long]()
        map += (offset_0._1.toString -> offset_0._2)
        map += (offset_1._1.toString -> offset_1._2)
        map += (offset_2._1.toString -> offset_2._2)
        HbaseUtils.putMapData(tableName, rowkey, "info", map)
        e.printStackTrace()
    }
    finally {
      if(semaphore == 1) {
        println("----comming in commited offset------")
        KafkaTools.setCommittedOffset(parameterTool.getRequired("inputtopic"), 0, offset_0._2)
        KafkaTools.setCommittedOffset(parameterTool.getRequired("inputtopic"), 1, offset_1._2)
        KafkaTools.setCommittedOffset(parameterTool.getRequired("inputtopic"), 2, offset_2._2)
      }

    }

    env.execute("app")
  }
}
