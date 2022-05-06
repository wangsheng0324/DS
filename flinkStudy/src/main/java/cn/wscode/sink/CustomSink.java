package cn.wscode.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * @author wangSheng
 * @title: CustomSink
 * @projectName DS
 * @description: TODO 自定义hbase连接器
 * @date 2022/3/30 14:54
 */
public class CustomSink {
    public static void main(String[] args) {
        RichSinkFunction<String> hbaseSink = new RichSinkFunction<String>() {
            //Hbase配置信息
            public Configuration configuration;
            //连接

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "localhost:2181");
                connection = ConnectionFactory.createConnection();
            }

            @Override
            public void close() throws Exception {
                super.close();
                connection.close();
            }

            public Connection connection;

            @Override
            public void invoke(String value, Context context) throws Exception {
                Table table = connection.getTable(TableName.valueOf("test"));
                Put put = new Put("rowKey".getBytes(StandardCharsets.UTF_8));
                put.addColumn("info".getBytes(StandardCharsets.UTF_8),//列
                        value.getBytes(StandardCharsets.UTF_8),//属性
                        "1".getBytes(StandardCharsets.UTF_8));
                table.put(put);
                table.close();
            }
        };
    }
}
