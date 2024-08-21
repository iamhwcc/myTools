package com.hwc.gmall.realtime.common.utils;

import com.hwc.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName, String groupID) {
        return "WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topicName + "', " +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "  'properties.group.id' = '" + groupID + "', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaTopicDB(String groupID) {
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n " +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<STRING,STRING>,\n" +
                "  `old` map<STRING,STRING>,\n" +
                "  `ts` BIGINT,\n" +
                "  proc_time as PROCTIME(),\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB, groupID);
    }

    public static String getKafkaSinkSQL(String topicName) {
        return "WITH (" +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topicName + "', " +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * 获取Upsert-Kafka连接，创建表一定要声明主键
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSinkSQL(String topicName) {
        return "WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topicName + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")";
    }

    public static String getDorisSinkSQL(String tableName) {
        return "WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '" + Constant.FENODES + "',\n" +
                "      'table.identifier' = '" + Constant.DORIS_DATABASE + "." + tableName + "',\n" +
                "      'username' = '" + Constant.DORIS_USER_NAME + "',\n" +
                "      'password' = '" + Constant.DORIS_PASSWORD + "',\n" +
                "      'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'\n" +
                ")";
    }
}
