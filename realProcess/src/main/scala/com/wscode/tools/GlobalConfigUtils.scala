package com.wscode.tools

import com.typesafe.config.ConfigFactory

/**
  * Created by angel
  */
class GlobalConfigUtils {

  private val conf = ConfigFactory.load()

  def bootstrapServers = conf.getString("bootstrap.servers")
  def zookeeperConnect = conf.getString("zookeeper.connect")
  def inputTopic = conf.getString("input.topic")
  def gruopId = conf.getString("gruop.id")
  def enableAutoCommit = conf.getString("enable.auto.commit")
  //auto.commit.interval.ms
  def commitInterval = conf.getString("auto.commit.interval.ms")
  //auto.offset.reset
  def offsetReset = conf.getString("auto.offset.reset")
  //Hbase

  def hbaseQuorem = conf.getString("hbase.zookeeper.quorum")
  def hbaseMaster = conf.getString("hbase.master")
  def clientPort = conf.getString("hbase.zookeeper.property.clientPort")
  def rpcTimeout = conf.getString("hbase.rpc.timeout")
  def operatorTimeout = conf.getString("hbase.client.operator.timeout")
  def scannTimeout = conf.getString("hbase.client.scanner.timeout.period")
}

object GlobalConfigUtils extends GlobalConfigUtils{}
