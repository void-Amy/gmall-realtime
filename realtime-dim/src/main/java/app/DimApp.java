package app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.deserializer.ContextObjectDeserializer;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import function.HBaseSinkFunction;
import function.TableProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/*
dim层维度层的处理
需要启动的进程：
mysql kafka(zk) -- hdfs(保存检查点) -- hbase(配置表信息)-dfs

 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {

        //直接调用继承过来的start方法
        new DimApp().start(10002,4,"dim_app",Constant.TOPIC_DB);
    }



    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaDS){
        //TODO 4.对流中的数据(topic_db中的数据)jsonString -> json Obj,并且进行ETL()

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            //获取操作的数据库名称
                            String db = jsonObject.getString("database");
                            //获取操作类型
                            String type = jsonObject.getString("type");
                            //获取当前操作影响的一条数据
                            String data = jsonObject.getString("data");
                            //ETL
                            if ("gmall1030".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2) {
                                //清洗后的数据往下游传递
                                out.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的JSON");
                        }
                    }
                }
        );
        //jsonObjDS.print("jsonObjDS:");
        return jsonObjDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env){
        //TODO 5.使用flink CDC读取配置表中的配置信息
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("gmall1030_config", "gmall1030_config.table_process_dim");
        // 5.2读取数据封装为流,这条流要做广播，它的并行度没有必要多个，直接设置为1就行
        DataStreamSource<String> mysqlStrDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "myssql_source").setParallelism(1);
        //mysqlStrDS.print("mysqlStrDS");

        // TODO 6.流中数据jsonString封装为java bean对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) throws Exception {
                        //增删改查是四种不同的数据
                        //为了处理方便，先将jsonStr转换成jsonObj
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        //获取对配置表中的数据进行的操作的类型：CRUD
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //删除数据操作：删除前的内容从before中获取到,并封装成TableProcessDim
                            //注意：alibaba fastjson底层自动帮我们做了下划线命名到驼峰命名的转换
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            //CRU操作，都是从当前json的after中获取配置内容
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

        //TableProcessDim(sourceTable=base_dic, sinkTable=dim_base_dic, sinkColumns=dic_code,dic_name, sinkFamily=info, sinkRowKey=dic_code, op=r)
        //tpDS.print("tpDS:");

        return tpDS;
    }


    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection hBaseConnection = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //创建连接
                        hBaseConnection = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        //关闭连接
                        HBaseUtil.closeHbaseCon(hBaseConnection);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim value) throws Exception {
                        //获取配置表中数据的操作类型
                        String op = value.getOp();

                        //获取操作的Hbase表的名字
                        String sinkTable = value.getSinkTable();
                        //获取在Hbase中的列族
                        String[] colFamilies = value.getSinkFamily().split(",");
                        if ("r".equals(op) || "c".equals(op)) {
                            //创建表
                            HBaseUtil.createTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, colFamilies);
                        } else if ("d".equals(op)) {
                            //从hbase中删除表
                            HBaseUtil.dropHbaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                        } else {
                            //先删除再创建，也就是update操作
                            HBaseUtil.dropHbaseTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createTable(hBaseConnection, Constant.HBASE_NAMESPACE, sinkTable, colFamilies);
                        }
                        return value;
                    }
                }
        ).setParallelism(1);
        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        //广播状态描述器，有这个下面处理的时候才能使用广播状态
        //key:表名，V:封装的一条配置表对象
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("tableProcess", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> ruleDS = tpDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(ruleDS);

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        //processDS.print("processDS:");

        return processDS;
    }

        @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaDS) {
        //TODO 4.对流中的数据(topic_db中的数据)jsonString -> json Obj,并且进行ETL()
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaDS);

        //TODO 5.使用flink CDC读取配置表中的配置信息
        // TODO 6.流中数据jsonString封装为java bean对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);


        // TODO 7.根据配置流中的数据到Hbase中执行建表或者删表操作
        tpDS = createHBaseTable(tpDS);

        // TODO 8.将配置流进行广播：broadcast
        // TODO 9.把主流业务数据与广播流配置信息进行关联--connect
        //TODO 10.关联后的数据进行处理--process(BroadCastProcessFunction)
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processDS = connect(tpDS, jsonObjDS);

            // TODO 11.将维度数据同步到hbase表中
        //flink没有提供连接器，自己创建
        processDS.addSink(
                new HBaseSinkFunction()
        );
    }
}


















