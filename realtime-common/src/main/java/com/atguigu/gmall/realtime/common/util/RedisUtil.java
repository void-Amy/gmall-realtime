package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutionException;

/**
 * 旁路缓存
 *  思路：先从缓存中获取维度数据，如果在缓存中获取到维度数据，直接将其做为返回值进行返回（缓存命中）
 *          如果在缓存中没有找到要关联的维度，发送请求到Hbase中获取维度数据，并将从hbase中查询到的维度数据放到缓存中缓存起来
 *          方便下次查询
 *  选型：
 *          状态      性能更好、维护性差
 *          Redis     性能也不错、维护性好
 *  关于redis中的一些设置：
 *          key: 维度表名：主键值 例如：dim_base_trademark:1
 *          value类型： String\set\zset\hash 此处选择string
 *          expire: 1day 避免冷数据常驻内存
 *          注意：如果业务数据库中维度表发生了变化，需要将缓存的维度数据清除
 */
public class RedisUtil {
    public static JedisPool jedisPool;

    static{
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
    }
    //获取Jedis
    public static Jedis getJedis(){
        System.out.println("开启jedis");
        //这个不支持异步操作
        return jedisPool.getResource();
    }

    public static void closeJedis(Jedis jedis){
        if(jedis != null){
            jedis.close();
        }
    }

    //获取异步操作redis的客户端
    public static StatefulRedisConnection<String,String> getAsyncRedisConnection(){
        System.out.println("开启redis异步客户端连接");
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        return redisClient.connect();
    }
    //关闭异步操作redis的客户端
    public static void closeAsyncRedisConnection(StatefulRedisConnection<String,String> asyncRedisConn){
        System.out.println("关闭redis异步客户端连接");
        if(asyncRedisConn != null && asyncRedisConn.isOpen()){
            asyncRedisConn.close();
        }
    }

    //从redis中查询维度数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        String key = gerRedisKey(tableName, id);
        //根据key到redis中获取对于的维度数据
        String dimJsonStr = jedis.get(key);
        if(StringUtils.isNotEmpty(dimJsonStr)){
            //缓存命中了
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        //redis中没有数据，返回null
        return null;
    }

    //向redis中放维度数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        String key = gerRedisKey(tableName, id);
        jedis.setex(key,24 * 3600,dimJsonObj.toJSONString());
    }

    //以异步的方式从Redis中读取数据
    public static JSONObject readDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id){
        String key = gerRedisKey(tableName, id);
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if(StringUtils.isNotEmpty(dimJsonStr)){
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    //以异步的方式向Redis中写入数据
    public static void writeDimAsync(StatefulRedisConnection<String,String> asyncRedisConn,String tableName,String id,JSONObject dimJsonObj){
        String key = gerRedisKey(tableName, id);
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        asyncCommands.setex(key,24 * 3600,dimJsonObj.toJSONString());
    }

    public static String gerRedisKey(String tableName,String id){
        String redisKey = tableName + ":" + id;
        return redisKey;
    }


    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
        closeJedis(jedis);

    }
}
