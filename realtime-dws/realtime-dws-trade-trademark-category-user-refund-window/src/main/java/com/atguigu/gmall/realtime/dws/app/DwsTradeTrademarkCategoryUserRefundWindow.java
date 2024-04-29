package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 交易域品牌-品类-用户粒度退单各窗口汇总表
 * 从 Kafka 读取退单明细数据，关联与分组相关的维度信息后分组，
 * 统计各分组各窗口的订单数和订单金额，补充与分组无关的维度信息，将数据写入 Doris 交易域品牌-品类-用户粒度退单各窗口汇总表
 *
 *
 */
public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                10031,
                4,
                "dws_trade_trademark_category_user_refund_window",
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        1）从 Kafka 退单明细主题读取数据
//        2）转换数据结构
//        JSONObject转换为实体类TradeTrademarkCategoryUserRefundBean。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanDS = kafkaDS.map(
                jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    TradeTrademarkCategoryUserRefundBean bean = TradeTrademarkCategoryUserRefundBean.builder()
                            .userId(jsonObj.getString("user_id"))
                            .orderIdSet(new HashSet<>(Collections.singleton(jsonObj.getString("order_id"))))
                            .skuId(jsonObj.getString("sku_id"))
                            .ts(jsonObj.getLong("ts") * 1000)
                            .build();
                    return bean;
                }
        );
//        3）补充与分组相关的维度信息

//（1）关联sku_info表
        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
//（2）获取tm_id，category3_id。
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTmAndCategory3DS = AsyncDataStream.unorderedWait(
                beanDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean refundObj, JSONObject dimJsonObj) {
                        refundObj.setTrademarkId(dimJsonObj.getString("tm_id"));
                        refundObj.setCategory3Id(dimJsonObj.getString("category3_id"));
                    }

                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean obj) {
                        return obj.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }
                },
                60,
                TimeUnit.SECONDS
        );
//        4）设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWatermarkDS = withTmAndCategory3DS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );
//        5）分组、开窗、聚合
//        按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间、结束时间以及当前统计时间。
//        6）补充与分组无关的维度信息
//（1）关联base_trademark表
//        获取tm_name。
//（2）关联base_category3表
//        获取name（三级品类名称），获取category2_id。
//（3）关联base_categroy2表
//        获取name（二级品类名称），category1_id。
//（4）关联base_category1 表
//        获取name（一级品类名称）。
    }
}
