package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 12.10交易域省份粒度下单各窗口汇总表
 *
 * 从Kafka读取订单明细数据，
 * 过滤null数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，将数据写入Doris交易域省份粒度下单各窗口汇总表。
 *
 * 维度：省份
 * 度量：订单数和订单金额
 *
 * 数据来源：dwd下单事实表
 *
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10030,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    /*
    {"create_time":"2024-04-27 09:32:40",
    "sku_num":"1",
    "split_original_amount":"8197.0000",
    "split_coupon_amount":"0.0",
    "sku_id":"10",
    "date_id":"2024-04-27",
    "user_id":"325",
    "province_id":"5",
    "sku_name":"Apple iPhone 12 (A2404) 64GB 蓝色 支持移动联通电信5G 双卡双待手机",
    "id":"14272601",
    "order_id":"61780",
    "split_activity_amount":"0.0",
    "split_total_amount":"8197.0",
    "ts":1714354360}
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //1）从Kafka下单明细主题读取数据
        //TODO 1.对六种数据类型进行转换，并且过滤null数据并
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(jsonStr)) {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            out.collect(jsonObject);
                        }
                    }
                }
        );
        //jsonObjDS.print();


        //TODO 2.按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> orderDetailKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> stateDescriptor =
                                new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //重复了,影响到度量值的字段进行取反
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        //distinctDS.print();



        //TODO 3。指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner((jsonObj, timestamp) -> jsonObj.getLong("ts") * 1000)
        );

        //TODO 4.再次对流中的数据类型转换 jsonObj -> 实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        String orderId = jsonObj.getString("order_id");
                        Long ts = jsonObj.getLong("ts");
                        Set<Object> idSet = new HashSet<>();
                        TradeProvinceOrderBean provinceOrderBean = TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .ts(ts)
                                .build();
                        return provinceOrderBean;
                    }
                }
        );

        //TODO 5.按照统计的维度分组
        KeyedStream<TradeProvinceOrderBean, String> keyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 6.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 7。聚合
        //到这一步真正重复的数据才被抵消掉
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        orderBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        orderBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        orderBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

        //TODO 8.关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reducedDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean obj) {
                        return obj.getProvinceId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withProvinceDS.print();

        //TODO 9.将关联结果写入doris中
        withProvinceDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSInkUtil.getDorisSink("dws_trade_province_order_window"));


    }
}











































