package app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.deserializer.ContextObjectDeserializer;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
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
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备基本环境
        // 1.1指定流处理环境
        // 1.2设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.检查点相关设置：
        // 2.1开启检查点,默认就是精准一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2设置检查点超时时间，
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000L);

        // 2.3设置job取消后检查点是否保留:要保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4设置两个检查点之间的最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // 2.5设置重启策略:如果检查点失败，重启
        //方式一：故障率重启？？？
        // 方式二：固定延迟重启：重启3次，每隔3000毫秒重启一次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000));
        // 2.6设置状态后端：指定检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        //检查点存储路径设置
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");

        // 2.7设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 3.从kafka主题中读取数据
        // 3.1声明消费者主题以及消费者组
        // 3.2创建kafkaSource
        //指定读取的数据类型是String(底层kafkareader帮我们消费数据，消费到的数据封装成String类型)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)//idea优化了：这里不用根据坐标从仓库中找common,可以直接从项目中找
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim_app_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                //生产环境从底层kafkareader维护的偏移量位置消费
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //kafka把消息封装成KV,我们主要读取V的部分
                //kafka中会有空消息，如果直接使用SimpleStringSchema()反序列化，空消息会出现问题（看他底层源码）
                //.setValueOnlyDeserializer(new SimpleStringSchema())//读数据的反序列化方式
                //自定义反序列化器
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if (message != null) {
                                    return new String(message);
                                }
                                //kafka中的空消息不会报错了（相比SimpleStringSchema()就是多了这一个判断）
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        // 3.3将读取的数据转为流:业务数据流，这里暂不指定水位线，WatermarkStrategy.noWatermarks()
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source");

        //TODO 4.对流中的数据(topic_db中的数据)jsonString -> json Obj,并且进行ETL()
        /*SingleOutputStreamOperator<JSONObject> JSONDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return jsonObject;
                    }
                }
        );*/
        //kafkaDS.print("kafkaDS:");

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


        //TODO 5.使用flink CDC读取配置表中的配置信息
        // 5.1创建mysqlSource对象
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall1030_config")
                .tableList("gmall1030_config.table_process_dim")
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                //.startupOptions()
                .build();
        // 5.2读取数据封装为流,这条流要做广播，它的并行度没有必要多个，直接设置为1就行
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "myssql_source").setParallelism(1);
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

        // TODO 7.根据配置流中的数据到Hbase中执行建表或者删表操作
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


        // TODO 8.将配置流进行广播：broadcast
        //广播状态描述器，有这个下面处理的时候才能使用广播状态
        //key:表名，V:封装的一条配置表对象
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("tableProcess", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> ruleDS = tpDS.broadcast(mapStateDescriptor);

        // TODO 9.把主流业务数据与广播流配置信息进行关联--connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(ruleDS);

        //TODO 10.关联后的数据进行处理--process(BroadCastProcessFunction)
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    Map<String, TableProcessDim> configMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //将配置表的配置信息提前预加载到程序中
                        //直接从mysql中把数据拿过来
                        //1.加载驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        //获取连接
                        String url = "jdbc:mysql://hadoop102:3306/gmall1030_config?useUnicode=true&characterEncoding=utf8&useSSL=true";
                        String username = "root";
                        String password = "000000";
                        java.sql.Connection connection = DriverManager.getConnection(url, username, password);
                        //5.获取数据库操作对象
                        String sql = "SELECT * FROM gmall1030_config.table_process_dim";
                        //预编译
                        PreparedStatement ps = connection.prepareStatement(sql);

                        //执行sql
                        ResultSet rs = ps.executeQuery();
                        //获取到虚表的表结构对象
                        ResultSetMetaData metaData = rs.getMetaData();

                        //处理结果，双层循环
                        while(rs.next()){
                            //行级循环，每一行的数据都放入到JSONJob对象中,然后再把这个jsonObj转换为TableProcesssDim对象
                            JSONObject jsonObj = new JSONObject();
                            //wo zhende fule
                            for(int i = 1;i <= metaData.getColumnCount(); ++i){
                                //列级循环
                                String columnLabel = metaData.getColumnLabel(i);//列名
                                Object columnValue = rs.getObject(columnLabel);//从当前游标中获取指定的列名的列值
                                jsonObj.put(columnLabel,columnValue);
                            }

                            //System.out.println(jsonObj.toJSONString());
                            //每一行的json对象都要转换成实体类
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);

                            //把这个实体类对象放入到map集合中
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }

                        //打印这个map对象
                        //System.out.println(configMap);

                        //6.释放连接
                        rs.close();
                        ps.close();
                        connection.close();

                    }

                    //处理topic_db中过来的数据流
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

                    //处理配置表中过来的规则流
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
                }
        );
        processDS.print("processDS:");

        // processElement:处理主流业务数据的，到广播状态中获取对应的配置信息，如果获取到了配置，说明是维度数据，将数据传递到下游
        // processBroadElement:处理广播流配置信息，将流中的配置信息放入到状态中

        // TODO 11.将维度数据同步到hbase表中
        //flink没有提供连接器，自己创建
        processDS.addSink(
                new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
                    Connection hbaseConnection = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //把hbase连接放到open方法中
                        hbaseConnection = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        //关闭hbase连接
                        HBaseUtil.closeHbaseCon(hbaseConnection);
                    }

                    //将流中的数据同步到Hbase对应的表中
                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcessDim> tup2, Context context) throws Exception {
                        //获取当前要同步的维度数据
                        JSONObject jsonObj = tup2.f0;

                        //获取当前这条维度数据对应的配置信息
                        TableProcessDim tableProcess = tup2.f1;

                        //获取业务数据库中对维度表的操作类型
                        String type = jsonObj.getString("type");
                        //获取表名,这个表名是hbase中的表名
                        String sinkTable = tableProcess.getSinkTable();

                        //获取rowkey:根据rowkey字段获取rowkey
                        String rowKey = jsonObj.getString(tableProcess.getSinkRowKey());

                        if ("delete".equals(type)) {
                            //从habase中删除对应的维度数据
                            HBaseUtil.delRow(hbaseConnection,Constant.HBASE_NAMESPACE,sinkTable,rowKey);
                        } else {
                            //往表中插入数据
                            HBaseUtil.putRow(hbaseConnection,Constant.HBASE_NAMESPACE,sinkTable,rowKey,tableProcess.getSinkFamily(),jsonObj);
                        }
                    }
                }
        );

        env.execute();
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


















