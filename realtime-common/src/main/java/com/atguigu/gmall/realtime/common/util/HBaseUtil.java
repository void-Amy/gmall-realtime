package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
}














