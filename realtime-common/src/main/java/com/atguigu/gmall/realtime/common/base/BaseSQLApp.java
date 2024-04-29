package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.time.Duration;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * Flink sql的基类
 */
public abstract class BaseSQLApp {
    public void start(int port, int parallelism,String ck){
        //基本环境准备
        //指定流处理环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //这里为什么并行度只设置1
        env.setParallelism(1);

        //检查点的相关设置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        System.setProperty("HADOOP_USER_NAME","atguigu");

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //设置状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //检查点存储位置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck"+ ck);

        //检查点并发数
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        //检查点之间最小间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(500);

        //检查点的超时时间
        checkpointConfig.setCheckpointTimeout(60 * 1000);

        //job取消的时候，检查点是否保存
        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        //指定表处理环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        //！！！设置状态的失效时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        handle(env,tableEnv);

    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) ;

    public TableResult readDB(StreamTableEnvironment tableEnv,String groupID){
        //准备动态表：
        // 方式一：准备流：从kafka中ods层读取数据topic_db，然后把流转换为表
        //方式二：使用connect连接器，直接创建动态表
        TableResult topicDB = tableEnv.executeSql("create table topic_db (\n" +
                "\t`database` string,\n" +
                "\t`table` string,\n" +
                "\t`type` string,\n" +
                "\t`data` map<string,string>,\n" +
                "\t`old` map<string,string>,\n" +
                "\t`ts` bigint,\n" +
                "\t`pt` as proctime(),\n" +
                "\tet as to_timestamp_ltz(ts,0),\n" +
                "\twatermark for et as et - interval '3' seconds\n" +
                ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DB, groupID));
        //topic_db.print();
        return topicDB;
    }

    public void readBaseDic(StreamTableEnvironment tableEnv){
        //关联维度数据：关联维度表base_dic,这张表存于HBase
        //1.先从hbase中把这张表给读出来
        TableResult baseDic = tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SQLUtil.getHBaseDDLSource(Constant.HBASE_NAMESPACE, "dim_base_dic", Constant.ZOOKEEPER_HOST));

    }
}
