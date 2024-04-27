package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradePaymentBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 12.7交易域支付成功各窗口汇总表（练习）
 * 从Kafka读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数。
 */
public class DwsTradePaymentSucWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradePaymentSucWindow().start(
                10027,
                4,
                "dws_trade_payment_suc_window",
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        1）从 Kafka支付成功明细主题读取数据
//        2）转换数据结构
//        String 转换为 JSONObject。
        //数据格式：
/*
{
  "order_detail_id": "14249687",
  "order_id": "47903",
  "user_id": "370",
  "sku_id": "4",
  "sku_name": "Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米",
  "province_id": "12",
  "activity_id": null,
  "activity_rule_id": null,
  "coupon_id": null,
  "payment_type_code": "1101",
  "payment_type_name": "支付宝",
  "callback_time": "2022-06-09 19:38:00",
  "sku_num": "1",
  "split_original_amount": "999.0000",
  "split_activity_amount": "0.0",
  "split_coupon_amount": "0.0",
  "split_payment_amount": "999.0",
  "ts": 1654774680
}
 */
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
//        3）设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
//        4）按照 user_id 分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
//        5）统计独立支付人数和新增支付人数
//        运用 Flink 状态编程，在状态中维护用户末次支付日期。
        SingleOutputStreamOperator<TradePaymentBean> beanDS = keyedDS.process(
                //往下游传递的是封装好的对象，只有是首次支付数据或者支付独立用户数据才给封装成对象往下游传递
                new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {
                    private ValueState<String> lastPaymentDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<String>("lastPaymentDate", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastPaymentDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TradePaymentBean>.Context ctx, Collector<TradePaymentBean> out) throws Exception {
                        //        若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；
                        //        否则首次支付用户数置为 0，判断末次支付日期是否为当日，如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
                        String lastPaymentDate = lastPaymentDateState.value();
                        // 支付成功独立用户数
                        Long paymentSucUniqueUserCount = 0L;
                        // 支付成功新用户数
                        Long paymentSucNewUserCount = 0L;

                        long ts = jsonObj.getLong("ts") * 1000;
                        String curDate = DateFormatUtil.tsToDate(ts);

                        if(StringUtils.isEmpty(lastPaymentDate)){
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                        }else {
                            if(!lastPaymentDate.equals(curDate)){
                                paymentSucUniqueUserCount = 1L;
                                lastPaymentDateState.update(curDate);
                            }
                        }

                        if(paymentSucUniqueUserCount != 0L || paymentSucNewUserCount != 0L){
                            TradePaymentBean tradePaymentBean = new TradePaymentBean(
                                    "",
                                    "",
                                    "",
                                    paymentSucUniqueUserCount,
                                    paymentSucNewUserCount,
                                    ts
                            );
                            out.collect(tradePaymentBean);
                        }
                    }
                }
        );
//        6）开窗、聚合
        AllWindowedStream<TradePaymentBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

//        度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
        SingleOutputStreamOperator<TradePaymentBean> reducedDS = windowDS.reduce(
                new ReduceFunction<TradePaymentBean>() {
                    @Override
                    public TradePaymentBean reduce(TradePaymentBean value1, TradePaymentBean value2) throws Exception {
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value2.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>.Context context, Iterable<TradePaymentBean> bean, Collector<TradePaymentBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());

                        TradePaymentBean tradePaymentBean = bean.iterator().next();
                        tradePaymentBean.setStt(stt);
                        tradePaymentBean.setEdt(edt);
                        tradePaymentBean.setCurDate(curDate);
                    }
                }
        );
//        7）写出到Doris
        reducedDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSInkUtil.getDorisSink("dws_trade_payment_suc_window"));
    }
}
