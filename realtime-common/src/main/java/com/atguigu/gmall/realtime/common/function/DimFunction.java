package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联需要实现的接口
 */
public interface DimFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getRowKey(T obj) ;
    String getTableName() ;
}
