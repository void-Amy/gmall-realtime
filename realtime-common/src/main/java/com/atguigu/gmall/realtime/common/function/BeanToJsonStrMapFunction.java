package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/*
将实体类对象转换成json字符串
 */
public class BeanToJsonStrMapFunction <T> implements MapFunction<T,String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        String jsonStr = JSON.toJSONString(bean, serializeConfig);
        return jsonStr;
    }
}
