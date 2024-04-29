package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * 互动域评论事务事实表
 *
 * 开发思路：
 * 1.基本环境准备：指定流处理环境，设置并行度，指定表执行环境
 * 2.检查点香瓜的设置：开启检查点，设置检查点的超时时间，设置job取消后检查点是否保留，设置两个检查点之间的最小时间间隔，设置重启策略，设置状态后端，设置操作hdfs的用户
 * 3.从kafka topic_db中读取数据，创建动态表 --kafka连接器
 * 4.过滤评论数据
 * 5.从hbase中查询字典数据  --Hbase连接器
 * 6.将评论表和字段表数据进行关联 --lookup join
 * 7.将关联数据写入到kafka主题中：先创建一个动态表，和写入的主题映射，执行写入操作 --upsert kafka连接器
 *
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,1,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        //从kafka中读取数据，创建动态表
        readDB(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //过滤出评论表的数据
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['spu_id'] spu_id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['appraise'] appraise,\n" +
                "\t`data`['comment_txt'] comment_txt,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\tts,\n" +
                "\tpt\n" +
                "from topic_db\n" +
                "where `database`='gmall1030'\n" +
                "and `table` = 'comment_info'\n" +
                "and `type` = 'insert'");


        //把得到的Table对象转换成临时表
        tableEnv.createTemporaryView("comment_info",commentInfo);
        //tableEnv.executeSql("select * from comment_info").print();

        //从hbase中读取base_dic
        readBaseDic(tableEnv);

        //对两张表进行join查询，补全评论事实表的维度，appraise判断是1 好评 2 中评 3 差评
        //使用lookup join
        Table joinedTable = tableEnv.sqlQuery("select \n" +
                "\tci.id,\n" +
                "\tci.user_id,\n" +
                "\tci.sku_id,\n" +
                "\tci.spu_id,\n" +
                "\tci.order_id,\n" +
                "\tci.appraise,\n" +
                "\tdic.dic_name appraise_name,\n" +
                "\tci.comment_txt,\n" +
                "\tci.ts\n" +
                "from comment_info as ci\n" +
                "join base_dic for system_time as of ci.pt as dic\n" +
                "on ci.appraise = dic.dic_code");

        //tableEnv.createTemporaryView("interaction_comment_info",interaction_comment_info);
        //tableEnv.executeSql("select * from " + joinedTable).print();

        //需要创建临时表和kafka映射-- upinsert kafka连接器(直接用kafka也可以，因为也不会有delete和update操作)
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + " (\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  spu_id string,\n" +
                "  order_id string,\n" +
                "  appraise string,\n" +
                "  appraise_name string,\n" +
                "  comment_txt string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO,Constant.KAFKA_BROKERS));

        //将查询到的结果写回到kafka对应的主题中
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);//往哪张表中写



    }
}
