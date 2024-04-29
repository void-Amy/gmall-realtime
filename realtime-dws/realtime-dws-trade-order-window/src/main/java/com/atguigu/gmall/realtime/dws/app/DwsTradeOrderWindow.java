package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeOrderBean;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 12.8交易域下单各窗口汇总表（练习）
 * 从 Kafka订单明细主题读取数据，统计当日下单独立用户数和首次下单用户数，封装为实体类，写入Doris。
 */
public class DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
                10028,
                4,
                "dws_trade_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        1）从 Kafka订单明细主题读取数据
//        2）转换数据结构
//        Kafka 订单明细主题的数据是通过 Kafka-Connector 从订单预处理主题读取后进行过滤获取的，Kafka-Connector 会过滤掉主题中的 null 数据，
//        因此订单明细主题不存在为 null 的数据，直接转换数据结构即可。
        //数据格式：
        /*
        {
  "id": "14249710",
  "order_id": "47905",
  "user_id": "23",
  "sku_id": "3",
  "sku_name": "小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机",
  "province_id": "19",
  "activity_id": "1",
  "activity_rule_id": "1",
  "coupon_id": null,
  "date_id": "2022-06-09",
  "create_time": "2022-06-09 19:37:59",
  "sku_num": "1",
  "split_original_amount": "6499.0000",
  "split_activity_amount": "500.0",
  "split_coupon_amount": "0.0",
  "split_total_amount": "5999.0",
  "ts": 1654774679
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

//        4）按照用户 id 分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
//        5）计算度量字段的值
//        运用 Flink 状态编程，在状态中维护用户末次下单日期。
        SingleOutputStreamOperator<TradeOrderBean> processDS = keyedDS.process(
                new ProcessFunction<JSONObject, TradeOrderBean>() {
                    private ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<String>("lastOrderDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastOrderDateState = getIterationRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                        // 若末次下单日期为 null，则将首次下单用户数和下单独立用户数均置为 1；
                        // 否则首次下单用户数置为 0，判断末次下单日期是否为当日，如果不是当日则下单独立用户数置为 1，否则置为 0。最后将状态中的下单日期更新为当日。
                        // 下单独立用户数
                        Long orderUniqueUserCount = 0L;
                        // 下单新用户数
                        Long orderNewUserCount = 0L;

                        long ts = jsonObj.getLong("ts") * 1000;
                        String curDate = DateFormatUtil.tsToDate(ts);

                        String lastOrderDate = lastOrderDateState.value();
                        if (StringUtils.isEmpty(lastOrderDate)) {
                            orderUniqueUserCount = 1L;
                            orderNewUserCount = 1L;
                        } else if (!lastOrderDate.equals(curDate)) {
                            orderUniqueUserCount = 1L;
                            lastOrderDateState.update(curDate);
                        }

                        if (orderUniqueUserCount != 0L || orderNewUserCount != 0L) {
                            TradeOrderBean tradeOrderBean = new TradeOrderBean(
                                    "",
                                    "",
                                    "",
                                    orderUniqueUserCount,
                                    orderNewUserCount,
                                    ts
                            );
                            out.collect(tradeOrderBean);
                        }
                    }
                }
        );

//        6）开窗、聚合
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
//        度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
        SingleOutputStreamOperator<TradeOrderBean> reducedDS = windowDS.reduce(
                new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>.Context context, Iterable<TradeOrderBean> elements, Collector<TradeOrderBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());

                        TradeOrderBean tradeOrderBean = elements.iterator().next();
                        tradeOrderBean.setStt(stt);
                        tradeOrderBean.setEdt(edt);
                        tradeOrderBean.setCurDate(curDate);

                        out.collect(tradeOrderBean);
                    }
                }
        );
//        7）写出到Doris。
        reducedDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSInkUtil.getDorisSink("dws_trade_order_window"));
    }
}
