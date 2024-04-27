package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.checkerframework.checker.units.qual.K;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;

/**
 * 12.9交易域SKU粒度下单各窗口汇总表
 *
 * 从Kafka订单明细主题读取数据，过滤null数据并按照唯一键对数据去重，
 * 按照SKU维度分组，统计原始金额、活动减免金额、优惠券减免金额和订单金额，
 * 并关联维度信息，将数据写入Doris交易域SKU粒度下单各窗口汇总表
 *
 * 维度：sku
 * 度量：原始金额、活动减免金额、优惠券减免金额和订单金额，
 *
 * 数据来源：dwd下单事实表（订单明细，订单表，订单活动，订单优惠券4张表组成）
 *          订单明细是主表，和订单表关联使用内连接
 *          和订单明细活动，订单明细优惠券关联使用的是左外连接
 *
 *          如果左外连接，左表数据先到，右表数据后到的时候，会产生3条数据
 *          左表      null +I
 *          左表      null -D
 *          左表      右表  +I
 *
 *          这样的动态表数据发送到kafka主题的话，kafka的下单事实表主题会接收到3条消息
 *          左表      null
 *          null
 *          左表      右表
 *
* 当Flink从Kafka的下单事实表中读取数据的时候，
 *          如果使用FlinkSQL的方式，会自动的处理空消息
 *          如果使用的FlinkAPI的方式，默认SimpleStringSchema类不能处理空消息的，需要手动定义反序列化器
 *
 * 另外，出了空消息外，第一条和第三条消息属于重复数据，我们在DWS程序中需要对其进行去重
 *          去重方案1：状态 + 定时器
 *          去重方案2：状态 + 抵消
 *
 *  维度关联：
 *      最基本维度关联
 *      优化1：旁路缓存
 *      优化2：异步IO
 *
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    /*
    数据格式：
    {
  "create_time": "2024-04-27 10:59:51",
  "sku_num": "1",
  "split_original_amount": "6499.0000",
  "split_coupon_amount": "0.0",
  "sku_id": "3",
  "date_id": "2024-04-27",
  "user_id": "310",
  "province_id": "18",
  "sku_name": "小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机",
  "id": "14268041",
  "order_id": "58973",
  "split_activity_amount": "0.0",
  "split_total_amount": "6499.0",
  "ts": 1714186791
}
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 1.处理空消息（flink其实也会自动处理，这里手动处理，提醒会有空消息），并且对流中数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(value)) {
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        }
                    }
                }
        );
        //jsonObjDS.print();

        //TODO 2.去重
        // 按照唯一键（订单明细ID）进行分组
        KeyedStream<JSONObject, String> orderDetailKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        // 去重方案1：状态 + 定时器
        // 这种方案时效性变差了，不管有没有重复的数据，每条数据都要等5s才能往下游传递
        //优点：如果重复，数据不会膨胀，只会传递一条数据
        /*
        SingleOutputStreamOperator<JSONObject> processedDS1 = orderDetailKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> stateDescriptor =
                                new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getIterationRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //状态中有上条数据，重复了，就判断时间谁大，将时间大的放到状态中去
                            String lastTs = lastJsonObj.getString("聚合时间戳");
                            String curTs = jsonObj.getString("聚合时间戳");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        } else {
                            //状态中没有上条数据，当前这条数据就是第一条，把当前数据放到状态中去，并且去注册5s后执行的定时器
                            lastJsonObjState.update(jsonObj);
                            TimerService timerService = ctx.timerService();
                            long currentProcessingTime = timerService.currentProcessingTime();//当前处理时间
                            timerService.registerProcessingTimeTimer(currentProcessingTime + 5000L);//当前处理时间的基础上推迟5s
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //定时器触发执行的方法
                        //把状态中的数据向下游传递
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        //清楚状态(所以上面的状态不用设置失效时间)
                        lastJsonObjState.clear();
                    }
                }
        );*/

        // 去重方案2：状态 + 抵消
        //优点：时效性好
        //缺点：如果数据重复了，传输3条数据
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
                            //数据重复了
                            //将状态中影响到度量值的字段取反然后传递到下游去
                            String splitOriginalAmount = jsonObj.getString("split_original_amount");
                            String splitCouponAmount = jsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = jsonObj.getString("split_activity_amount");
                            String splitTotalAmount = jsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);

                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        //distinctDS.print();

        //TODO 3.指定WaterMark生成策略以及提取事件时间生成字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
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

        //TODO 4.流中数据进行转换 jsonObj -> 实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                        String skuId = jsonObj.getString("sku_id");
                        String splitOriginalAmount = jsonObj.getString("split_original_amount");
                        String splitCouponAmount = jsonObj.getString("split_coupon_amount");
                        String splitActivityAmount = jsonObj.getString("split_activity_amount");
                        String splitTotalAmount = jsonObj.getString("split_total_amount");

                        return TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(new BigDecimal(splitOriginalAmount))
                                .couponReduceAmount(new BigDecimal(splitCouponAmount))
                                .activityReduceAmount(new BigDecimal(splitActivityAmount))
                                .orderAmount(new BigDecimal(splitTotalAmount))
                                .build();
                    }
                }
        );
//        beanDS.print();

        //TODO 5.按照统计维度sku进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 6.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 7.聚合计算
        SingleOutputStreamOperator<TradeSkuOrderBean> reducedDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                //补充时间属性
                new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());

                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);

                        out.collect(orderBean);
                    }
                }
        );
        //reducedDS.print();

        //TODO 8.关联sku维度，
        //维度关联最基本的实现
        /*
        reducedDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hBaseConnection;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConnection = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseCon(hBaseConnection);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据维度的主键获取对于的维度对象
                        JSONObject dimJsonObj = HBaseUtil.getRow(hBaseConnection,
                                Constant.HBASE_NAMESPACE,
                                "dim_sku_info",
                                skuId,
                                JSONObject.class);
                        //将维度对象的属性补全到流中对象上
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));

                        return orderBean;
                    }
                }
        );*/
        //优化1：加旁路缓存
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reducedDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hBaseConnection;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConnection = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseCon(hBaseConnection);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度主键
                        String skuId = orderBean.getSkuId();
                        //先从redis中获取要关联的维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);

                        if (dimJsonObj != null) {
                            //如果从redis中获取到了维度数据（缓存命中）
                            System.out.println("从redis中缓存命中了");
                        } else {
                            //从hbase中查询
                            //如果没有缓存命中，需要从hbase中查询数据，并且把查询到的数据放入到redis中缓存起来
                            dimJsonObj = HBaseUtil.getRow(hBaseConnection, Constant.HBASE_NAMESPACE,
                                    "dim_sku_info",
                                    skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                System.out.println("从hbase中查询到了数据");
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("没有找到要关联的数据");
                            }

                        }

                        //将维度对象相关的数据补充到流中的对象上
                        if (dimJsonObj != null) {
                            //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        }
                        return orderBean;
                    }
                }
        );
        //输出数据：
        //TradeSkuOrderBean(orderDetailId=null, stt=2024-04-27 15:48:10,
        // edt=2024-04-27 15:48:20, curDate=2024-04-27,
        // trademarkId=8, trademarkName=null, category1Id=null,
        // category1Name=null, category2Id=null, category2Name=null,
        // category3Id=477, category3Name=null, skuId=26,
        // skuName=索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ,
        // spuId=9, spuName=null, originalAmount=774.0000, activityReduceAmount=0.0,
        // couponReduceAmount=122.61, orderAmount=651.39, ts=null)
        //withSkuInfoDS.print();


        //TODO 9.关联spu维度

        //TODO 10.关联tm维度

        //TODO 11.关联category3维度
        //TODO 12.关联category2维度
        //TODO 13.关联category1维度

        //TODO 14.将关联结果写入doris中
    }
}














































