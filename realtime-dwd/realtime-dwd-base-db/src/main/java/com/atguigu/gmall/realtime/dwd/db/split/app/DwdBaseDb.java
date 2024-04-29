package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSInkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import com.atguigu.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.jline.utils.InfoCmp.Capability.columns;


/**
 * 事实表动态分流处理
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {

        new DwdBaseDb().start(10019,4,"dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //对主流中的数据做类型转换jsonStr -> jsonObj,并且做简单的etl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }
                    }
                }
        );
        //jsonObjDS.print("jsonODBDS");


        //使用flink CDC从配置表中读取配置信息
        //创建Mysqlsource对象
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource(
                "gmall1030_config", "gmall1030_config.table_process_dwd");
        //读取数据，封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        //mysqlStrDS.print("mysqlStrDS");

        //对当前流中的数据类型进行转换：jsonSTR -> 实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        //先转换为JSONObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);

                        //获取对配置表的操作类型
                        String op = jsonObj.getString("op");

                        //获取数据中有用的部分，CRUD,如果是CRU，数据在after部分，如果是d，删除前的数据在before中
                        TableProcessDwd tableProcess = null;
                        if("d".equals(op)){
                            //删除配置表中的一条数据
                            tableProcess = jsonObj.getObject("before", TableProcessDwd.class);
                        }else {
                            tableProcess = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tableProcess.setOp(op);
                        return tableProcess;

                    }
                }
        );
        //tableProcessDS.print("tableProcessDS");


        //广播配置表的数据 --broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDwd>(
                "mapStateDescriptor",String.class,TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> tableProcessDwdBroadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        //将主流数据和配置表数据进行connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectedDS = jsonObjDS.connect(tableProcessDwdBroadcastStream);


        //对关联后的数进行process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processed = connectedDS.process(
                new BaseDbTableProcessFunction(mapStateDescriptor)
        );

        //将流中的数据写到kafka不同的主题中
        processed.sinkTo(FlinkSInkUtil.getKafkaSink());



    }



}

























