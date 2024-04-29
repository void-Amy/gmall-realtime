package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {

        new DwdTradeCartAdd().start(10013,1, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        //从kafka的topic_db主题中读取数据 创建动态表topic_db表
        readDB(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //过滤出加购行为
        Table cartAddTable = tableEnv.sqlQuery("select\n" +
                "`data`['id'],\n" +
                "`data`['user_id'],\n" +
                "`data`['sku_id'],\n" +
                "`data`['cart_price'],\n" +
                "if(`type` = 'insert',`data`['sku_name'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'cart_info'\n" +
                "and (`type` = 'insert' or (`type` = 'update' and `old`['sku_num'] is not null)\n" +
                "and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))");

        //将过滤出来的加购数据写入到kafka主题
        //创建映射kafka主题的临时表
        tableEnv.executeSql("CREATE TABLE " + Constant.TOPIC_DWD_TRADE_CART_ADD +" (\n" +
                "  id STRING,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  cart_price string,\n" +
                "  sku_num string,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD,Constant.KAFKA_BROKERS));

        cartAddTable.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
