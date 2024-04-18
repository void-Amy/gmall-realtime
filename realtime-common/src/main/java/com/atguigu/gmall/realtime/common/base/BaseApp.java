package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId,String topic){
        //TODO 1.准备基本环境
        // 1.1指定流处理环境
        // 1.2设置并行度

        //如果您不指定 REST 端口号，Flink 将会使用默认的 REST 端口号 8081。
        //REST 端口号用于提供 Flink 的 Web 界面，可以通过该界面查看 Flink 程序的运行状态、任务管理、度量指标等信息
        Configuration config = new Configuration();
        config.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(parallelism);

        //TODO 2.检查点相关设置：
        // 2.1开启检查点,默认就是精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2设置检查点超时时间，
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);

        // 2.3设置job取消后检查点是否保留:要保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4设置两个检查点之间的最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 2.5设置重启策略:如果检查点失败，重启
        //方式一：故障率重启？？？
        // 方式二：固定延迟重启：重启3次，每隔3000毫秒重启一次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        // 2.6设置状态后端：指定检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        //检查点存储路径设置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");

        // 2.7设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3.从kafka主题中读取数据
        // 3.1声明消费者主题以及消费者组
        // 3.2创建kafkaSource
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);

        // 3.3将读取的数据转为流:业务数据流，这里暂不指定水位线，WatermarkStrategy.noWatermarks()
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");
        //kafkaDS.print("kafkaDS");

        //拿到kafka中的数据流周，其它的就由子类自行决定做做什么操作
        handle(env,kafkaDS);


        //提交作业
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) ;
}
