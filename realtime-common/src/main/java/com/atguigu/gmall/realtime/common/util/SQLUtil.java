package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaDDLSource(String topic,String groupId){
        return "WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = '" + topic + "',\n" +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "'properties.group.id' = '" + groupId + "',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'json.ignore-parse-errors' = 'true',\n" +//当json解析失败的时候，忽略这条数据
                "'format' = 'json'\n" +
                ")";
    }

    public static String getHBaseDDLSource(String nameSpace,String tableName,String zkhost){
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + nameSpace + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = '" + zkhost + "',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL' ,\n" +
                " 'lookup.partial-cache.max-rows' = '500' ,\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour' ,\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour' \n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic,String kafkaBrokers){
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ topic + "',\n" +
                "  'properties.bootstrap.servers' = '"+ kafkaBrokers + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    public static String getDorisSinkDDL(String tableName){
        return "   WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = '"+Constant.DORIS_FE_NODES+"', " +
                "  'table.identifier' = '"+Constant.DORIS_DATABASE+"."+tableName+"', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " +
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")  ";
    }
}
