package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 流量域版本-渠道-地区-访客类别is_new粒度页面浏览各窗口汇总表
 *
 * 维度：版本-渠道-地区-访客类别is_new
 * 度量：本节汇总表中需要有会话数、页面浏览数、浏览总时长、独立访客数四个度量字段
 *
 * 数据来来源：页面日志事实表
 *
 * 为了统计方便，可以定义一个统计的实体类，用于封装维度和度量的内容
 *
 * 每从页面日志事实表中读取一条数据，就封装一个统计的实体类对象（相当于wordcount转换二元组的过程）
 * 假设读到一条日志数据，就new一个对象出来
 * new TrafficPageViewBean(
 * vc -- common
 * ch -- common
 * ar -- common
 * is_new -- common
 *
 * pv -- 1L
 * dur -- page[during_time]
 * sv -- page[last_page_id] is null就是开了新的会话，而不是从别的地方跳过来的，计数
 * uV --(例如页面被访问100次，代表pv100次，但是这100次就是10个人访问的，uu就是10（按照用户区分），uv(按照设备区分))
 *      需要借助flink状态编程
 *
 * 指定watermark生成策略以及提取事件时间
 * 按照统计的维度进行分组
 * 开窗
 * 聚合计算
 *
 * 将结果写入doris
 *
 * )
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 对流中的数据类型进行转换 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 按照mid进行分组（统计UV）
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 对分组后的数据进行处理 jsonObj -> 实体类对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        //只要判断当前设备今天来没来过
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        String vc = jsonObj.getJSONObject("common").getString("vc");
                        String ch = jsonObj.getJSONObject("common").getString("ch");
                        String ar = jsonObj.getJSONObject("common").getString("ar");
                        String is_new = jsonObj.getJSONObject("common").getString("is_new");

                        JSONObject pageObj = jsonObj.getJSONObject("page");

                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        //从状态中获取上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        Long uvCt = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }

                        String lastPageId = pageObj.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                        Long duringTime = pageObj.getLong("during_time");
                        TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                is_new,
                                uvCt,
                                svCt,
                                1L,
                                duringTime,
                                ts
                        );

                        out.collect(trafficPageViewBean);
                    }
                }
        );
        //beanDS.print();

        //TODO 指定watermark的生成策略以及提取事件时间
        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 按照统计的维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWaterMarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(
                                bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew()
                        );
                    }
                }
        );


        //TODO 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowedDS =
                dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        //TODO 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reducedDS = windowedDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean pageViewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);
                        out.collect(pageViewBean);
                    }
                }
        );
        //reducedDS.print("reduceDS");


        //TODO 将聚合的结果写入doris中
        reducedDS
                .map(
                       new BeanToJsonStrMapFunction<>()
                )
                .sinkTo(
                FlinkSInkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));

    }
}




























