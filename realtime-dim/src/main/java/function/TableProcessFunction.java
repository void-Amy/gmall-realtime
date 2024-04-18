package function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Table;

import java.sql.*;
import java.util.*;


public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    Map<String, TableProcessDim> configMap = new HashMap<>();

    MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = null;

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor){
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表的配置信息提前预加载到程序中
        //直接从mysql中把数据拿过来
        //将配置表中的配置信息提前加载到程序中configMap
        Connection conn = JdbcUtil.getMysqlConnection();
        String sql = "select * from gmall1030_config.table_process_dim";
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(conn, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(conn);

    }
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //获取广播状态,这里获取到的广播状态是只读的
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //从当前处理的主流对象中获取表名
        String table = jsonObj.getString("table");
        //根据表名到广播状态中获取对应的配置对象,如果广播流中没有，则去预加载的map中的去拿数据
        TableProcessDim tableProcessDim = null;
        if ((tableProcessDim = broadcastState.get(table)) != null
                || (tableProcessDim = configMap.get(table)) != null) {
            //说明当前处理的主流数据是维度数据，将其data部分传递到下游
            JSONObject dataJsonObject = jsonObj.getJSONObject("data");
            //在向下游传递数据前，把不需要传递的属性过滤掉，根据配置表中的sinkcolumns
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObject, sinkColumns);

            //补充对维度表数据的操作类型(需要知道最下游hbase表做什么操作)
            String type = jsonObj.getString("type");
            dataJsonObject.put("type", type);
            //将当前维度信息和维度配置信息封装成Tuple2传递到下游
            out.collect(Tuple2.of(dataJsonObject, tableProcessDim));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        //1.获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取对配置表的操作的类型(空指针？)
        String op = tableProcessDim.getOp();
        String key = tableProcessDim.getSourceTable();
        if ("d".equals(op)) {
            //说明从配置表中删除一条数据，这条数据对于的配置信息要从状态中删除
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            //说明对配置表进行了C|R|U操作，要将这条配置信息放到广播状态中
            broadcastState.put(key, tableProcessDim);
            configMap.put(key,tableProcessDim);
        }
    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObject, String sinkColumns) {
       /* List<String> list = Arrays.asList(sinkColumns.split(","));
        JSONObject newJsonObj = new JSONObject();


        for (String column : list) {
            newJsonObj.put(column,dataJsonObject.getString(column));
        }*/

        //方法二：
        /*Set<Map.Entry<String, Object>> entries = dataJsonObject.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            if(!list.contains(entry.getKey())){
                entries.remove(entry);

            }
        }*/
        List<String> list = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = dataJsonObject.entrySet();
        entries.removeIf(entry -> !list.contains(entry.getKey()));
    }

}
