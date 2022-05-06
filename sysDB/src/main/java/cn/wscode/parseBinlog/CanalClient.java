package cn.wscode.parseBinlog;

import cn.wscode.producer.KafkaSend;
import cn.wscode.tools.GlobalConfigUtils;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by angel
 */
public class CanalClient {
    public static void main(String[] args) {
        String host = GlobalConfigUtils.host;
        int port = Integer.parseInt(GlobalConfigUtils.port);
        String instance = GlobalConfigUtils.instance;
        String username = GlobalConfigUtils.username;
        String password = GlobalConfigUtils.password;
        CanalConnector conn = getConn("hadoop01", 11111, "example", "root", "root");
        //连接上canal之后，开始订阅canal的binlog日志
        int batchSize = 100;
        int emptyCount = 1;

        try {
            conn.connect();
            conn.subscribe(".*\\..*");//
            conn.rollback();
            int totalCount = 120; //循环次数
            while (totalCount > emptyCount) {
                //获取数据
                Message message = conn.getWithoutAck(batchSize);
                long id = message.getId();
                int size = message.getEntries().size();
                if (id == -1 || size == 0) {
                    //没有读取到任何数据
                } else {
                    //有数据，那么解析binlog日志
                    analysis(message.getEntries(), emptyCount);
                    emptyCount++;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.disconnect();
        }
    }

    public static void analysis(List<CanalEntry.Entry> entries, int emptyCount) {
        for (CanalEntry.Entry entry : entries) {
            //mysql的事务开始前 和事务结束后的内容  不要的
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            //如果不是以上的事务，那么解析binlog
            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
            //获取关键字段 哪一个数据库有事务发生  那张表 、 增加  删除  修改
            CanalEntry.EventType eventType = rowChange.getEventType();//操作的是insert 还是delete 还是update
            String logfileName = entry.getHeader().getLogfileName();//当前读取的是哪一个binlog文件
            long logfileOffset = entry.getHeader().getLogfileOffset();//当前读取的binlog文件位置
            String dbName = entry.getHeader().getSchemaName();//当前操作的mysql数据库
            String tableName = entry.getHeader().getTableName();//当前操作的是哪一张表


            //迭代所有获取到的binlog数据，然后根据当前mysql的INSERT  UPDATE  DELETE操作，进行解析
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                //判断：当前是什么操作
                if (eventType == CanalEntry.EventType.DELETE) {
                    dataDetails(rowData.getBeforeColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, emptyCount);
                    //当前是删除操作
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, emptyCount);
                } else {
                    //update
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbName, tableName, eventType, emptyCount);
                }
            }
        }
    }

    private static void dataDetails(List<CanalEntry.Column> columns, String logFileName, Long logFileOffset,
                                    String dbName, String tableName, CanalEntry.EventType eventType, int emptyCount) {
        //找到当前那些列发生了改变  以及改变的值
        List<Object> list1 = new ArrayList<Object>();
        for (CanalEntry.Column column : columns) {
            List<Object> list2 = new ArrayList<Object>();
            list2.add(column.getName());//当前发生改变的列
            list2.add(column.getValue());//当前列发生改变的值
            list2.add(column.getUpdated());//是否更改 或者插入 或者删除  成功
            list1.add(list2);
        }

        //将处理后的细节发送到kafka
        String key = UUID.randomUUID().toString();
        String data = logFileName + "#CS#" + logFileOffset + "#CS#" + dbName + "#CS#" + tableName + "#CS#"
                + eventType + "#CS#" + list1 + emptyCount;
        System.out.println(data);
        KafkaSend.sendMessage("canal", key, data);

    }


    //连接canal
    public static CanalConnector getConn(String host, int port, String instance, String username, String password) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), instance, username, password);
        return canalConnector;
    }
}
