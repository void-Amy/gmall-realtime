package com.atguigu.gmall.realtime.common.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.connector.kafka.source.KafkaSource;

public class FlinkSourceUtil {
    /**
     * 获取kafkaSource
     * @param topic
     * @param groupId
     * @return
     */
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){
        return null;
    }

    public static MySqlSource<String> getMysqslSource(String dbname,String... tableName){
        return null;
    }


}
