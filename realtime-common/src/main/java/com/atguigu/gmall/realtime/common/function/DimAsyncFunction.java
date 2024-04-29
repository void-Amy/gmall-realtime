package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T>{
    //异步IO优化
    private AsyncConnection asyncHbaseConnection;
    private StatefulRedisConnection<String,String> asyncRedisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncHbaseConnection = HBaseUtil.getAsyncHBaseConnection();
        asyncRedisConn = RedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeAsyncHbaseCon(asyncHbaseConnection);
        RedisUtil.closeAsyncRedisConnection(asyncRedisConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //创建异步编排对象  执行线程任务，有返回结果，这个返回结果将做为下一个线程任务的入参
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        //先从redis中获取要关联的维度数据
                        String key = getRowKey(obj);
                        String tableName = getTableName();
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(asyncRedisConn, tableName, key);
                        return dimJsonObj;
                    }
                }
        ).thenApplyAsync(
                //有入参，有返回值
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        //如果从redis中找到了要关联的维度--缓存命中，直接把当前数据返回就行
                        //如果没有从redis中找到要关联的维度，则发送请求到hbase中获取维度，并将这条维度数放到redis中缓存起来方便下次查询使用
                        if(dimJsonObj != null){
                            System.out.println("从Redis中获取到了" + getTableName() + "的" + getRowKey(obj) + "数据");
                        }else {
                            dimJsonObj = HBaseUtil.getRowAsync(asyncHbaseConnection,Constant.HBASE_NAMESPACE,getTableName(),getRowKey(obj));
                            if(dimJsonObj != null){
                                System.out.println("从HBase中获取到了" + getTableName() + "的" + getRowKey(obj) + "数据");
                                RedisUtil.writeDimAsync(asyncRedisConn,getTableName(),getRowKey(obj),dimJsonObj);
                            }else {
                                System.out.println("从HBase中没有找到" + getTableName() + "的" + getRowKey(obj) + "数据");
                            }
                        }
                        return dimJsonObj;

                    }
                }
        ).thenAcceptAsync(
                //有入参，无返回值
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        //将维度对象相关的数据补充到流中的对象上
                        if(dimJsonObj != null){
                            addDims(obj,dimJsonObj);
                        }
                        //将关联后的数据向下游传递
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        throw new RuntimeException("如果异步关联超时，可以做如下检查：\n" +
                "1.是否将相关组件都启动了：zk,kafka,hdfs,hbase,redis\n" +
                "2.查看Redis中bind的配置是否注掉或者是0.0.0.0\n" +
                "3.查看Hbase维度表中是否存在维度数据，如果没有，要把维度数据同步一下\n" +
                "4.找老师帮助");
    }



}
