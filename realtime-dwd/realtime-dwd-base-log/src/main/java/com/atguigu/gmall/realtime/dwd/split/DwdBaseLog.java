package com.atguigu.gmall.realtime.dwd.split;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;


public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {

        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 1.数据简单的ETL
        SingleOutputStreamOperator<JSONObject> etledStream = etl(kafkaDS);
        //etledStream.print("etl");

        //TODO 2.纠正新老客户
        SingleOutputStreamOperator<JSONObject> validatedStream  = validateNewOrOld(etledStream);
        //validatedStream.print("纠正新老客户");

        //TODO 3.分流
        Map<String, DataStream<String>> streams = splitStream(validatedStream);

        //TODO 4.不同的流写入到不同kafka主题中
        writeToKafka(streams);
    }


    /**
     * 1.数据简单的ETL
     * 定义测流输出流标签--用于标记脏数据
     * ETL
     * 将侧输出流中的脏数据写到kafka对应的主题中
     * @param kafkaDS
     * @return
     */
    public static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaDS){
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };

        //对流中的数据进行解析，将字符串转换为JSONObject,
        // 如果解析报错则必然为脏数据。定义侧输出流，将脏数据发送到侧输出流，写入Kafka脏数据主题
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //放入到测输出流中
                            ctx.output(dirtyTag,jsonStr);
                        }
                    }
                }
        );
        //jsonObjDS.print("主流数据：");
        SideOutputDataStream<String> diryStream = jsonObjDS.getSideOutput(dirtyTag);

        //侧流数据也要写到kafak主题中
        writeToKafka("dwd_dirty",diryStream);
        return jsonObjDS;
    }



    /**
     * 纠正新老客户
     * 思路：运用Flink状态编程，为每个mid维护一个键控状态，记录首次访问日期
     *
     * 按照设备id进行分组
     * 修复
     *
     * @param etledStream
     * @return
     * {
     *   "common": {
     *     "ar": "370000",
     *     "ba": "Honor",
     *     "ch": "wandoujia",
     *     "is_new": "1",
     *     "md": "Honor 20s",
     *     "mid": "eQF5boERMJFOujcp",
     *
     *   "ts": 1585744304000
     * }
     */
    public static SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> etledStream){
        //按照设备id来做分组
        KeyedStream<JSONObject, String> keyedDS =
                etledStream.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObj) throws Exception {
                        return jsonObj.getJSONObject("common").getString("mid");
                    }
                }
        );
        //keyedDS.print("keyedDS");

        //键控状态的设置：键控状态使用值状态
        //1.is_new = 1,初始为是新用户，1表示是新用户，is_new肯定要保存到状态中，如果是普通变量的话，它的作用范围是整个算子
        //      如果firstVisitDate == null,则firstVisitDate == ts
        //      如果firstVisitDate ！= null,且首次访问日期不是当日(ts),说明是老用户了，修改is_new = 0;
        //      如果firstVisitDate ！= null,且首次访问日期是当日(ts),说明是今天刚访问的，还是新用户，不做操作
        //2.is_new = 0,已经是老用户了
        //      如果firstVisitDate == null,则firstVisitDate == 昨日？？？为什么要设置昨日(数仓基于业务系统做的，数仓上限之前的log数据不会被保存，之后数仓中才会保存log数据，所以这里可能会是null,这里可以随便加一个值只要可以代表曾经访问过就可以了)
        //      如果firstVisitDate ！= null,不做操作
        SingleOutputStreamOperator<JSONObject> processed = keyedDS.process(
                //key, In,Out
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> firstVisitDateState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> firstVisitDateStateDes = new ValueStateDescriptor<>("firstVisitDateState", String.class);
                        firstVisitDateState = getRuntimeContext().getState(firstVisitDateStateDes);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从流总数据取出is_new和ts字段
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        Long ts = jsonObj.getLong("ts");
                        //从流状态中取出初始访问日期
                        String firstVisitDate = firstVisitDateState.value();
                        //转换为天
                        String curVistiDate = DateFormatUtil.tsToDate(ts);

                        if("1".equals(isNew)){
                            //是新用户
                            //if(firstVisitDate == null){
                            if (StringUtils.isEmpty(firstVisitDate)) {
                                firstVisitDateState.update(curVistiDate);
                            }else if(!StringUtils.isEmpty(firstVisitDate) && !firstVisitDate.equals(curVistiDate)){
                                //jsonObj.put("common",jsonObj.getJSONObject("common").put("is_new", 0));
                                isNew = "0";
                                jsonObj.getJSONObject("common").put("is_new", 0);
                            }
                        }else {
                            //是老用户
                            if(StringUtils.isEmpty(firstVisitDate)){
                                //数仓刚上限，没有记录之前的访问日期
                                //随便更新一个日期，只要能代表之前访问过就行了
                                long yesterday = ts - 24 * 60 * 60 * 1000;
                                //firstVisitDateState.update(DateFormatUtil.dateAdd(curVistiDate,-1));
                                firstVisitDateState.update(DateFormatUtil.tsToDate(yesterday));
                            }
                        }
                        out.collect(jsonObj);
                    }
                }
        );
        return processed;
    }

    /**
     * 分流
     * @param validatedStream
     * @return
     */
    public static Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream){

        //定义分流标签
        //泛型擦除，要用匿名内部类的方式创建，或者在声明类型
        OutputTag<String> start = new OutputTag<String>("start"){};

        //流量域曝光事务事实表
        OutputTag<String> display = new OutputTag<String>("display"){};
        //流量域动作事务事实表
        OutputTag<String> action = new OutputTag<String>("action"){};
        //流量域错误事务事实表
        OutputTag<String> err = new OutputTag<String>("err"){};
        //流量域页面浏览事务事实表

        //侧输出流必须用process,因为要用到上下文对象，只有process才能拿到上下文对象
        SingleOutputStreamOperator<String> pageDS = validatedStream.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //分流逻辑
                        //1.首先获取 err 字段，如果返回值不为 null 则将整条日志数据发送到错误侧输出流
                        //String errStr = jsonObj.getString("err");
                        JSONObject errObj = jsonObj.getJSONObject("err");
                        if (errObj != null) {//不会比较对象的地址，它底层会自动 toString
                            ctx.output(err, jsonObj.toJSONString());//如果走了这一步，就不会执行后面的了吧
                            //这只是写测输出流啊，当然会接着往下走了
                            jsonObj.remove("err");//这个字段没有用了，删掉
                        }

                        //2.处理启动日志：判断是否有 start 字段，如果有则说明数据为启动日志，将其发送到启动侧输出流
                        JSONObject startObj = jsonObj.getJSONObject("start");
                        if (startObj != null) {
                            ctx.output(start, jsonObj.toJSONString());
                        } else {
                            //处理页面日志
                            //获取页面日志中都应该有的三个属性：common page ts
                            JSONObject commonObj = jsonObj.getJSONObject("common");
                            JSONObject pageObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            //获取曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            //判断当前页面上是否由曝光
                            if (displayArr != null && displayArr.size() > 0) {
                                //遍历曝光数组，获取每一条曝光数
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);

                                    //定义一个新的json对象，封装曝光数
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonObj);
                                    newDisplayJsonObj.put("page", pageObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);

                                    //一条日志数据变成了好多条数据写出去
                                    ctx.output(display, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);

                                    //定义新的JSON封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonObj);
                                    newActionJsonObj.put("page", pageObj);
                                    newActionJsonObj.put("action", actionJsonObj);

                                    ctx.output(action, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            //最后页面就是将页面日志，写到主流中
                            out.collect(jsonObj.toJSONString());
                        }

                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(err);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(start);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(display);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(action);

        //测试
        errDS.print("错误");
        startDS.print("启动");
        displayDS.print("曝光");
        actionDS.print("动作");
        pageDS.print("页面");

        //把这些封装成map集合返回会去
        HashMap<String, DataStream<String>> dsMap = new HashMap<>();
        dsMap.put(Constant.TOPIC_DWD_TRAFFIC_ERR,errDS);
        dsMap.put(Constant.TOPIC_DWD_TRAFFIC_START,startDS);
        dsMap.put(Constant.TOPIC_DWD_TRAFFIC_DISPLAY,displayDS);
        dsMap.put(Constant.TOPIC_DWD_TRAFFIC_ACTION,actionDS);
        dsMap.put(Constant.TOPIC_DWD_TRAFFIC_PAGE,pageDS);

        return dsMap;
    }

    /**
     * 将流写入到kafka对应的主题中去
     * @param dsMap
     */
    public static void writeToKafka(Map<String, DataStream<String>> dsMap){
        for (Map.Entry<String, DataStream<String>> ds : dsMap.entrySet()) {
            writeToKafka(ds.getKey(),ds.getValue());
        }
    }

    public static <T> void writeToKafka(String topic ,DataStream<T> stream){
        KafkaSink kafkaSink = FlinkSInkUtil.getKafkaSink(topic);
        stream.sinkTo(kafkaSink);
    }
    
    
}
