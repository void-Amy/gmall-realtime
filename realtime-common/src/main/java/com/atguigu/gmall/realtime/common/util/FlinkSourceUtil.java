package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

public class FlinkSourceUtil {
    /**
     * 获取kafkaSource
     * @param topic
     * @param groupId
     * @return
     */
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){
        //指定读取的数据类型是String(底层kafkareader帮我们消费数据，消费到的数据封装成String类型)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)//idea优化了：这里不用根据坐标从仓库中找common,可以直接从项目中找
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                //生产环境从底层kafkareader维护的偏移量位置消费
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //kafka把消息封装成KV,我们主要读取V的部分
                //kafka中会有空消息，如果直接使用SimpleStringSchema()反序列化，空消息会出现问题（看他底层源码）
                //.setValueOnlyDeserializer(new SimpleStringSchema())//读数据的反序列化方式
                //自定义反序列化器
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                //kafka中的空消息不会报错了（相比SimpleStringSchema()就是多了这一个判断）
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        return kafkaSource;
    }

    public static MySqlSource<String> getMysqlSource(String dbname,String... tableName){
        // 5.1创建mysqlSource对象
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList(dbname)
                .tableList(tableName)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                //.startupOptions()
                .build();
        return mySqlSource;
    }


}
