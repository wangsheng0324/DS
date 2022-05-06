package cn.wscode.tableAsql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author wangSheng
 * @title: externalSystem
 * @projectName DS
 * @description: TODO sql查询的外部系统
 * @date 2022/4/18 18:04
 */
public class externalSystem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1、本地文件 filesystem
        String localDDl = "create table clickTable(" +
                "`name` String, " +
                "`url` String, " +
                "ts BIGINT) with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'E:\\maven_space\\DS\\flinkStudy\\input\\clicks.csv', " +
                " 'format' = 'csv')";
        tableEnv.executeSql(localDDl);

        //2、系统打印 print
        String printDDl = "create table printOutTable(" +
                "`name` String, " +
                "`url` String) with (" +
                " 'connector' = 'print')";
        tableEnv.executeSql(printDDl);

        //3、kafka 只能插入
        String kafkaDDl = "create table kafkaTable(" +
                "`name` String, " +
                "`url` String ," +
                "ts TIMESTAMP(3) METADATA FROM 'timestamp') with (" +
                " 'connector' = 'kafka', " +
                " 'topic' = 'test', " +
                " 'properties' = 'bootstrap.servers=localhost:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'properties.auto.offset.reset' = 'latest' ," +
                " 'format' = 'json')";
        tableEnv.executeSql(kafkaDDl);

        // 4、upsertKafka 有更新操作的kafka
        String upsertKafkaDDl = "CREATE TABLE pageviews_per_region (" +
                "user_region STRING," +
                "pv BIGINT, " +
                "uv BIGINT, " +
                "PRIMARY KEY (user_region) NOT ENFORCED " +
                ") WITH (" +
                " 'connector' = 'upsert-kafka', " +
                " 'topic' = 'test', " +
                " 'properties' = 'bootstrap.servers=localhost:9092'," +
                "'key.format' = 'avro'," +
                "'value.format' = 'avro' )";
        tableEnv.executeSql(upsertKafkaDDl);

        //5、JDBC
        String jdbcDDl = "CREATE TABLE MyTable ( " +
                " id BIGINT, " +
                " name STRING, " +
                " age INT, " +
                " status BOOLEAN, " +
                " PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH ( " +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://localhost:3306/mydatabase', " +
                " 'table-name' = 'users' " +
                "); ";
        tableEnv.executeSql(jdbcDDl);
        //6、Es
        String esDDl = "CREATE TABLE MyTable ( " +
                " user_id STRING, " +
                " user_name STRING " +
                " uv BIGINT, " +
                " pv BIGINT, " +
                " PRIMARY KEY (user_id) NOT ENFORCED " +
                ") WITH ( " +
                " 'connector' = 'elasticsearch-7', " +
                " 'hosts' = 'http://localhost:9200', " +
                " 'index' = 'users' " +
                "); ";
        tableEnv.executeSql(esDDl);
        //7、Hbase
        String hbaseDDl = "CREATE TABLE MyTable ( " +
                "rowkey INT, " +
                "family1 ROW<q1 INT>, " +
                "family2 ROW<q2 STRING, q3 BIGINT>, " +
                "family3 ROW<q4 DOUBLE, q5 BOOLEAN, q6 STRING>, " +
                "PRIMARY KEY (rowkey) NOT ENFORCED " +
                ") WITH ( " +
                "'connector' = 'hbase-1.4', " +
                "'table-name' = 'mytable', " +
                "'zookeeper.quorum' = 'localhost:2181' " +
                ");";
        tableEnv.executeSql(hbaseDDl);
        //8、hive
        //8.1、配置hiveCatalog
        String name = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir = "/opt/hive-conf";
        // 创建一个 HiveCatalog，并在表环境中注册
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // 使用 HiveCatalog 作为当前会话的 catalog
        tableEnv.useCatalog("myhive");

        // 配置 hive 方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // 配置 default 方言
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String hiveDDl = "SET table.sql-dialect=hive; " +
                "CREATE TABLE hive_table ( " +
                " user_id STRING, " +
                " order_amount DOUBLE " +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (" +
                " 'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00', " +
                " 'sink.partition-commit.trigger'='partition-time', " +
                " 'sink.partition-commit.delay'='1 h', " +
                " 'sink.partition-commit.policy.kind'='metastore,success-file' " +
                ");";


    }
}
