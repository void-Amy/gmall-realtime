package com.atguigu.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {
    Map<String,TableProcessDwd> configMap = new HashMap<>();
    MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor){
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //使用jdbc
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mysqlConnection,
                "select * from gmall1030_config.table_process_dwd",
                TableProcessDwd.class,
                true);

        //对结果进行处理，全部放入configMap中
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String key = getKey(tableProcessDwd.getSourceTable() , tableProcessDwd.getSourceType());
            configMap.put(key,tableProcessDwd);
        }

        //最后关闭连接
        JdbcUtil.closeConnection(mysqlConnection);

    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据规则流中的数据对主流数据进行处理
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        String key = getKey(table, type);

        TableProcessDwd tableProcess = null;
        if( (tableProcess = broadcastState.get(key)) != null
                || (tableProcess = configMap.get(key)) != null){
            //数据传递到下游：只传递data部分
            JSONObject dataObj = jsonObj.getJSONObject("data");
            //传递前，将多余的字段过滤掉
            deleteNotNeedColumn(dataObj,tableProcess.getSinkColumns());
            //将要对数据的操作加入进来
            //在向下游传递前，补充ts字段
            dataObj.put("ts",jsonObj.getLong("ts"));
            out.collect(Tuple2.of(dataObj,tableProcess));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据流中的数据对广播状态做出处理:Create增加 Read查询 Update更新 Delete删除
        String op = tableProcessDwd.getOp();
        String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
        if("d".equals(op)){
            //删除状态中的一条数据
            broadcastState.remove(key);
            configMap.remove(key);
        }else {
            //增加一条数据
            broadcastState.put(key,tableProcessDwd);
            configMap.put(key,tableProcessDwd);
        }
    }

    public String getKey(String sourceTable,String sourceType){
        //key之所以这么设计是应为要过滤主流中的数据是否是属于这个表的这个类型(insert,update)!!!!
        return sourceTable + ":" + sourceType;
    }

    private void deleteNotNeedColumn(JSONObject jsonObj, String sinkColumns){
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        jsonObj.entrySet().removeIf(entry -> !columnList.contains(entry.getKey()));
    }
}
