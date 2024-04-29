package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 操作Hbase
 */
public class HBaseUtil {
    //获取hbase连接
    public static Connection getHBaseConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //端口默认就是2181,不用设置
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            Connection connection = ConnectionFactory.createConnection(conf);
            return connection;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //关闭连接
    public static void closeHbaseCon(Connection con) throws IOException {
        if(con != null){
            con.close();
        }
    }

    //获取异步操作Hbase的连接
    public static AsyncConnection getAsyncHBaseConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //端口默认就是2181,不用设置
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //
    public static void closeAsyncHbaseCon(AsyncConnection asyncConnection){
        if(asyncConnection != null && !asyncConnection.isClosed()){
            try {
                asyncConnection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //提供建表方法
    public static void createTable(Connection connection, String nameSpace,String tableName,String... colFamilies){
        if(colFamilies.length < 1){
            System.out.println("在hbase中建表必须指定列族");
            return;
        }

        try(Admin admin = connection.getAdmin()) {
            //判断表是否存在
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if(admin.tableExists(tableNameObj)){
                System.out.println("要创建的表" + nameSpace + ":" + tableName + "已经存在");
                return;
            }

            //获取列族
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>(colFamilies.length);
            for (String colFamily : colFamilies) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build();
                columnFamilyDescriptorList.add(columnFamilyDescriptor);
            }

            //表描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptorList).build();
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //提供删表方法
    public static void dropHbaseTable(Connection connection, String nameSpace,String tableName) {

        try( Admin admin = connection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //从habase表中put数据

    /**
     *
     * @param connection 连接对象
     * @param nameSpace 表空间
     * @param tableName 表名
     * @param rowKey
     * @param columnFamilies 列族
     * @param jsonObj 要写入的json对象
     */
    public static void putRow(Connection connection, String nameSpace, String tableName, String rowKey, String columnFamilies, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try(Table table = connection.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));

            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if(value != null){
                    put.addColumn(Bytes.toBytes(columnFamilies),Bytes.toBytes(column),Bytes.toBytes(value));
                }
            }

            table.put(put);
            System.out.println("向"+ nameSpace + ":" + tableName + "插入数据" + jsonObj + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //从habase表中删除数据
    public static void delRow(Connection connection, String nameSpace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try(Table table = connection.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("删除"+ nameSpace + ":" +tableName + "表中的数据:"+ rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //更具rowkey从表中查询数据
    //方法的返回只是任意类型
    //<T> 声明泛型
    public static <T> T getRow(Connection hbaseConn, String nameSpace,String tableName,String rowKey,
                               Class<T> tClass,boolean... isUnderlineToCamel){
        boolean defaultIsUToC = false;

        if(isUnderlineToCamel.length > 0){
            defaultIsUToC = isUnderlineToCamel[0];

        }

        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                T obj = tClass.newInstance();
                for (Cell cell : cells) {
                    byte[] bytes = CellUtil.cloneQualifier(cell);
                    String columnName = Bytes.toString(bytes);
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));

                    if(defaultIsUToC){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    //以异步的方式从Hbase中根据rowkey获取维度对象
    public static JSONObject getRowAsync(AsyncConnection asyncConnection, String nameSpace,String tableName,String rowKey){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConnection.getTable(tableNameObj);
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    byte[] bytes = CellUtil.cloneQualifier(cell);
                    String columnName = Bytes.toString(bytes);
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName,columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;

    }


    public static void main(String[] args){
        Connection hBaseConnection = getHBaseConnection();
        JSONObject dimSkuInfo = HBaseUtil.getRow(hBaseConnection,
                Constant.HBASE_NAMESPACE,
                "dim_sku_info",
                "10",
                JSONObject.class);
        System.out.println(dimSkuInfo.toJSONString());
        try {
            closeHbaseCon(hBaseConnection);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}














