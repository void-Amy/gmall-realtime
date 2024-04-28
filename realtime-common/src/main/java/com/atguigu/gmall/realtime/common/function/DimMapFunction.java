package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import com.atguigu.gmall.realtime.common.function.DimFunction;

/**
 * 旁路缓存关联维度模板
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T, T> implements DimFunction<T>{
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
    public T map(T obj) throws Exception {
        //根据流中的对象获取要关联的维度主键
        String key = getRowKey(obj);
        //先从redis中获取要关联的维度数据
        //如果从redis中找到了要关联的维度--缓存命中，直接把当前数据返回就行
        //如果没有从redis中找到要关联的维度，则发送请求到hbase中获取维度，并将这条维度数放到redis中缓存起来方便下次查询使用
        String tableName = getTableName();
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, tableName, key);

        if(dimJsonObj != null){
            System.out.println("从redis中获取到了" + tableName + "的" + key + "数据");
        }else {
            dimJsonObj = HBaseUtil.getRow(hBaseConnection, Constant.HBASE_NAMESPACE,tableName,key, JSONObject.class);
            if(dimJsonObj != null){
                System.out.println("从Hbase中获取到了" + tableName + "的" + key + "数据");
                RedisUtil.writeDim(jedis,tableName,key,dimJsonObj);
            }else {
                System.out.println("从Hbase中没有找到" + tableName + "的" + key + "数据");
            }
        }

        //将维度属性补充到流中对象上去
        if(dimJsonObj != null){
            addDims(obj,dimJsonObj);
        }
        return obj;
    }
}
