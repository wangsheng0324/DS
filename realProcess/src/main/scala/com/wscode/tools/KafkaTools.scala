package com.wscode.tools

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import java.util
import java.util.Properties

/**
 * @title: KafkaTools
 * @projectName DS
 * @description: TODO ⼿动维护kafka的偏移量
 * @author wangSheng
 * @date 2022/4/26 16:54
 */
object KafkaTools {
  var offsetClient: KafkaConsumer[Array[Byte], Array[Byte]] = null
  var properties: Properties = null

  def initOffsetHandler(): Unit = {
    properties = new Properties
    properties.setProperty("bootstrap.servers", GlobalConfigUtils.bootstrapServers)
    properties.setProperty("zookeeper.connect", GlobalConfigUtils.zookeeperConnect)
    properties.setProperty("group.id", GlobalConfigUtils.gruopId)
    properties.setProperty("enable.auto.commit", "false") //自动提交
    properties.setProperty("auto.offset.reset", "latest")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    offsetClient = new KafkaConsumer[Array[Byte], Array[Byte]](properties)
  }

  //返回（分区号，偏移量）
  def getCommittedOffset(topicName: String, partition: Int): (Int, Long) = {
    initOffsetHandler()
    val committed = offsetClient.committed(new TopicPartition(topicName,
      partition))
    println(topicName, partition, committed.offset())
    if (committed != null) {
      (partition, committed.offset)
    } else {
      (partition, 0L)
    }
  }

    //更改偏移量
  def setCommittedOffset(topicName: String, partition: Int, offset: Long) {
    initOffsetHandler()
    var partitionAndOffset: util.Map[TopicPartition, OffsetAndMetadata] = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    partitionAndOffset.put(new TopicPartition(topicName, partition), new OffsetAndMetadata(offset))
    offsetClient.commitSync(partitionAndOffset)
  }

  def close() {
    offsetClient.close()
  }

}
