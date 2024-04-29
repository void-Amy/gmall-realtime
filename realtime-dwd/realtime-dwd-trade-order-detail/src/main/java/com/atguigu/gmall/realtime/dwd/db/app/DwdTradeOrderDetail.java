package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        //一定要设置状态保留时间,为什么要+5
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30 * 60 +5));

        //读取kafka中topic_db数据创建topic_db表
        readDB(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        //过滤出订单表
        /*
    select
    `data`['id']                     ,
    `data`['user_id']                ,
    `data`['province_id']            ,
    `data`['activity_reduce_amount'] ,
    `data`['coupon_reduce_amount']   ,
    `data`['original_total_amount']  ,
    from topic_db
    where `table` = 'order_info'
    and `type` = 'insert'
         */
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "    `data`['id'] id                    ,\n" +
                "    `data`['user_id'] user_id              ,\n" +
                "    `data`['province_id']  province_id          ,\n" +
                "    `data`['activity_reduce_amount'] activity_reduce_amount,\n" +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount  ,\n" +
                "    `data`['original_total_amount'] original_total_amount \n" +
                "    from topic_db\n" +
                "    where `table` = 'order_info'\n" +
                "    and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info",orderInfo);

        //过滤出订单明细表
        Table orderDetail = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['sku_name'] sku_name," +
                        "data['create_time'] create_time," +
                        "data['source_id'] source_id," +
                        "data['source_type'] source_type," +
                        "data['sku_num'] sku_num," +
                        "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                        "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "data['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "data['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "data['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts " +
                        "from topic_db " +
                        "where `table`='order_detail' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail",orderDetail);

        //过滤出订单明细活动表
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['activity_id'] activity_id, " +
                        "data['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `table`='order_detail_activity' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_activity",orderDetailActivity);

        //过滤出订单明细优惠券表
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where  `table`='order_detail_coupon' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_coupon",orderDetailCoupon);

        //把4张表进行关联
        Table result = tableEnv.sqlQuery(
                "select \n" +
                        "od.id,\n" +
                        "od.order_id,\n" +
                        "oi.user_id,\n" +
                        "od.sku_id,\n" +
                        "od.sku_name,\n" +
                        "oi.province_id,\n" +
                        "act.activity_id,\n" +
                        "act.activity_rule_id,\n" +
                        "cou.coupon_id,\n" +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +  // 年月日
                        "od.create_time,\n" +
                        "od.sku_num,\n" +
                        "od.split_original_amount,\n" +
                        "od.split_activity_amount,\n" +
                        "od.split_coupon_amount,\n" +
                        "od.split_total_amount,\n" +
                        "od.ts \n" +
                        "from order_detail od \n" +
                        "join order_info oi on od.order_id=oi.id \n" +//内连接
                        "left join order_detail_activity act \n" +//左连接
                        "on od.id=act.order_detail_id \n" +
                        "left join order_detail_coupon cou \n" +
                        "on od.id=cou.order_detail_id ");

        //把最后关联的数据写到kafka主题
        //首先创建映射kafka主题的临时表
        tableEnv.executeSql( "create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(" +
                "id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "date_id string," +
                "create_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_total_amount string," +
                "ts bigint," +
                "primary key(id) not enforced " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.KAFKA_BROKERS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
