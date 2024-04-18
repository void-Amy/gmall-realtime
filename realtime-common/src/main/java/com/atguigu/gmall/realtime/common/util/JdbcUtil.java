package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.atguigu.gmall.realtime.common.constant.Constant.*;

/**
 * 从遵行JDBC规范的数据库表中查询数据
 */
public class JdbcUtil {

    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        //1.加载驱动
        Class.forName(MYSQL_DRIVER);
        //获取连接
        return DriverManager.getConnection(MYSQL_URL, MYSQL_USER_NAME, MYSQL_PASSWORD);
    }

    public static <T>List<T>  queryList(Connection conn,
                                        String querySql,
                                        Class<T> tClass,
                                        boolean... isUnderlineToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //默认不执行下划线转驼峰
        boolean defaultIsUToC = false;
        if(isUnderlineToCamel.length > 0){
            defaultIsUToC = isUnderlineToCamel[0];
        }

        List<T> result = new ArrayList<>();
        //预编译
        PreparedStatement ps = conn.prepareStatement(querySql);

        //执行sql
        ResultSet rs = ps.executeQuery();
        //获取到虚表的表结构对象
        ResultSetMetaData metaData = rs.getMetaData();

        //处理结果，双层循环
        while(rs.next()){
            //行级循环，每一行的数据都放入到JSONJob对象中,然后再把这个jsonObj转换为TableProcesssDim对象
            T t = tClass.newInstance();
            //wo zhende fule
            for(int i = 1;i <= metaData.getColumnCount(); ++i){
                //列级循环
                String columnLabel = metaData.getColumnLabel(i);//列名
                Object columnValue = rs.getObject(columnLabel);//从当前游标中获取指定的列名的列值

                if(defaultIsUToC){
                    //需要转驼峰
                    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnLabel);
                }

                BeanUtils.setProperty(t,columnLabel,columnValue);
            }

            result.add(t);

        }

        return result;


    }

    public static void closeConnection(Connection conn) throws SQLException {
        //6.释放连接
        if(conn != null && !conn.isClosed()){
            conn.close();
        }
    }


}









































