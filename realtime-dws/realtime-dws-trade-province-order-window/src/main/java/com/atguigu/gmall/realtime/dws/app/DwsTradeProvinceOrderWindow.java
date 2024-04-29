package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 12.10交易域省份粒度下单各窗口汇总表
 *
 * 从Kafka读取订单明细数据，
 * 过滤null数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，将数据写入Doris交易域省份粒度下单各窗口汇总表。
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

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
//        1）从Kafka下单明细主题读取数据
//        2）过滤null数据并转换数据结构
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
//        3）按照唯一键去重(订单明细id)


//        4）转换数据结构
//        JSONObject转换为实体类TradeProvinceOrderWindow。
//        5）设置水位线
//        6）按照省份ID分组
//        provinceId可以唯一标识数据。
//        7）开窗
//        8）聚合计算
//        度量字段求和，并在窗口闭合后补充窗口起始时间、结束时间以及当前统计日期。
//        9）关联省份信息
//        补全省份名称字段。
//        10）写出到Doris。
    }
}
