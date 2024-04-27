package function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

        Connection hbaseConnection = null;
        Jedis jedis;

        @Override
        public void open(Configuration parameters) throws Exception {
            //把hbase连接放到open方法中
            hbaseConnection = HBaseUtil.getHBaseConnection();
            jedis = RedisUtil.getJedis();
        }

        @Override
        public void close() throws Exception {
            //关闭hbase连接
            HBaseUtil.closeHbaseCon(hbaseConnection);
            RedisUtil.closeJedis(jedis);
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
                HBaseUtil.delRow(hbaseConnection, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
            } else {
                //往表中插入数据
                HBaseUtil.putRow(hbaseConnection,Constant.HBASE_NAMESPACE,sinkTable,rowKey,tableProcess.getSinkFamily(),jsonObj);
            }
            //如果业务数据库中的维度表发生了变化，要将redis中的缓存数据清楚掉
            if("delete".equals(type) || "update".equals(type)){
                jedis.del(sinkTable + ":" + rowKey);
            }
        }

}
