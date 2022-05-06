package com.wscode.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by angel
  */
object HbaseUtils {
  //1: 封装hbase的参数
  private val config: Configuration = HBaseConfiguration.create()
  /*
  hbase.zookeeper.quorum="hadoop01:2181,hadoop02:2181,hadoop03:2181"
hbase.master="hadoop01:60000"
hbase.zookeeper.property.clientPort="2181"
hbase.rpc.timeout="600000"
hbase.client.operator.timeout="600000"
  * */
  config.set("hbase.zookeeper.quorum" , GlobalConfigUtils.hbaseQuorem)
  config.set("hbase.master" , GlobalConfigUtils.hbaseMaster)
  config.set("hbase.zookeeper.property.clientPort" , GlobalConfigUtils.clientPort)
  config.set("hbase.rpc.timeout" , GlobalConfigUtils.rpcTimeout)
  config.set("hbase.client.operator.timeout" , GlobalConfigUtils.operatorTimeout)
  //def scannTimeout = conf.getString("c")
  config.set("hbase.client.scanner.timeout.period" , GlobalConfigUtils.scannTimeout)
  //2:构建hbase的连接操作
  private val conn: Connection = ConnectionFactory.createConnection(config)
  //3:获取hbase的客户端操作
  private val admin: Admin = conn.getAdmin

  //4:初始化函数
  def Init(tableName: TableName , columnFamily:String):Table = {
    //1):构建表的描述器
    val hTableDescriptor = new HTableDescriptor(tableName)
    //2）：构建列族的描述器
    val hColumnDescriptor = new HColumnDescriptor(columnFamily)
    hTableDescriptor.addFamily(hColumnDescriptor)
    //3）：如果表不存在则创建表
    if(!admin.tableExists(tableName)){
      //如果不存在，那么创建表
      admin.createTable(hTableDescriptor)
    }
    conn.getTable(tableName)
  }

  //查询数据操作--根据rowkey
  def getData(tableName: TableName , rowkey:String , columnFamily:String , column:String):String = {
    val table: Table = Init(tableName , columnFamily)
    var tmp = ""
    try{
      val bytesRowkey = Bytes.toBytes(rowkey)
      val get: Get = new Get(bytesRowkey)
      val result: Result = table.get(get)
      val values: Array[Byte] = result.getValue(Bytes.toBytes(columnFamily) ,Bytes.toBytes(column.toString))
      if( values != null && values.size > 0 ){
        tmp = Bytes.toString(values)
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      table.close()
    }
    tmp
  }

  //插入数据操作-1
  def putData(tableName: TableName , rowKey:String , columnFamily:String , column:String , data:String) = {
    val table: Table = Init(tableName , columnFamily)
    try{
      val put: Put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily) ,Bytes.toBytes(column.toString) , Bytes.toBytes(data.toString))
      table.put(put)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      table.close()
    }
  }

  //插入数据操作-2
  def putMapData(tableName: TableName , rowKey:String , columnFamily:String , mapData:Map[String , Long]) = {
    val table: Table = Init(tableName , columnFamily)
    try{
      val put: Put = new Put(Bytes.toBytes(rowKey))
      if(mapData.size > 0){
        for((k , v) <- mapData){
          put.addColumn(Bytes.toBytes(columnFamily) ,Bytes.toBytes(k.toString) , Bytes.toBytes(v.toString))
        }
      }
      table.put(put)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      table.close()
    }
  }




  def main(args: Array[String]): Unit = {
//    putData(TableName.valueOf("test") , "123" , "info" , "tt" , "this is test")
//    val data = getData(TableName.valueOf("test") , "123" , "info" , "tt")
//    print(data)
    var map = Map[String , Long]()
    map += ("t1" -> 123L)
    map += ("t2" -> 234L)
    putMapData(TableName.valueOf("test2") , "123" , "info" , map)
  }
}
