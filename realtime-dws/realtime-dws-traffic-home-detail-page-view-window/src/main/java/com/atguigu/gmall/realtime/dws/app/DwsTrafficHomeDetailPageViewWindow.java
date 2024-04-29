package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

/**
 * 12.3流量域首页、详情页页面浏览各窗口汇总表
 * 维度：没有维度
 * 过滤条件:首页、详情页
 * 度量：当日的独立访客数
 *
 * 开发步骤：
 * 1.从页面日志主题读取数据
 * 2.过滤出首页和详情页
 * 3.判断是否为独立访客
 * 4.分组、开窗、聚合
 * 5.将聚合结果写到Doris中
 *
 */
public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(
                10023,
                4,
                "dws_traffic_home_detail_page_view_window",
                //TODO 1.消费kafka DWD页面主题数据
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 2.转换数据结构 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 3.过滤数据：仅保留page_id为home或good_detail的数据，其它数据无用
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObj ->
                "home".equals(jsonObj.getJSONObject("page").getString("page_id"))
                        || "good_detail".equals(jsonObj.getJSONObject("page").getString("page_id")));

        //filterDS.print();


        //TODO 4.设置水位线以及提起事件时间字段
        SingleOutputStreamOperator<JSONObject> withWaterMarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );


        //TODO 5.按照mid分组
        KeyedStream<JSONObject, String> keyedDS = withWaterMarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 4.使用flink的状态编程，判断是否是首页详情页的独立访客，并把json对象转换成实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(
                new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

                    //因为度量的是Uv,所以要利用状态来排除来存放每一个mid是否访问过了
                    //状态中要记录上次访问home 和good_detail的时间，状态保留一天
                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> goodDetailLastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> homeStateDescriptor =
                                new ValueStateDescriptor<String>("homeLastVisitDateState", String.class);
                        homeStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        homeLastVisitDateState = getRuntimeContext().getState(homeStateDescriptor);

                        ValueStateDescriptor<String> goodDetailStateDescriptor =
                                new ValueStateDescriptor<String>("goodDetailStateDescriptor", String.class);
                        goodDetailStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        goodDetailLastVisitDateState = getRuntimeContext().getState(goodDetailStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        Long homeUv = 0L;
                        Long goodDetailUv = 0L;

                        if ("home".equals(pageId)) {
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                homeUv = 1L;
                                homeLastVisitDateState.update(curVisitDate);
                            }
                        } else {
                            String goodDetailLastVisitDate = goodDetailLastVisitDateState.value();
                            if (StringUtils.isEmpty(goodDetailLastVisitDate) || !goodDetailLastVisitDate.equals(curVisitDate)) {
                                goodDetailUv = 1L;
                                goodDetailLastVisitDateState.update(curVisitDate);
                            }

                        }

                        //只有是首页或者详情页的独立访客才有必要向下游传递
                        if (homeUv != 0L || goodDetailUv != 0L) {
                            TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean =
                                    new TrafficHomeDetailPageViewBean(
                                            "",
                                            "",
                                            "",//ts不行，如果出现了跨天，就乱了，最好根据窗口的时间来确定curDate,这里统计周期是天，如果用ts影响还不大，但是如果统计周期不是天，用ts就乱了
                                            homeUv,
                                            goodDetailUv,
                                            ts
                                    );
                            out.collect(trafficHomeDetailPageViewBean);
                        }

                    }
                }
        );
        //一条数据，homeUvCt和goodDetailUvCt不会同时为0，也不会同时为1
        //BeanDS.print("beanDS");


        //TODO 开窗,没有维度，就是所有并行度都在一个窗口里
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        //TODO 分组聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }
                ,
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());

                        TrafficHomeDetailPageViewBean bean = elements.iterator().next();
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);

                        out.collect(bean);
                    }
                }
        );

        //TODO 把聚合后的数据写入doris中
        reduceDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSInkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));
    }
}
























