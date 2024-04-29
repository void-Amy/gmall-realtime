package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,1, "dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        //注册自定义的函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //从dwd的页面日志主题中读取数据，创建动态表，并指定watermark生产策略
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "                `common` map<string,string>,\n" +
                "                `page` map<string,string>,\n" +
                "                `ts` bigint,\n" +
                "                et as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "                 WATERMARK FOR et AS et\n" +
                ")" + SQLUtil.getKafkaDDLSource(Constant.TOPIC_DWD_TRAFFIC_PAGE, Constant.KAFKA_BROKERS));
        //tableEnv.executeSql("select * from page_log").print();

        //过滤出搜索行为:什么行为是搜索行为
        //last_page_id是search,item表示搜索的关键词，item_type：keyword表示是关键词搜索
        /*
        select
            page['item'] fullword,
            et
        from page_log
        where page['last_page_id'] = 'search'
        and page['item_type'] = 'keyword'
        and page['item'] is not null
         */
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "            page['item'] full_word,\n" +
                "            et\n" +
                "        from page_log\n" +
                "        where page['last_page_id'] = 'search'\n" +
                "        and page['item_type'] = 'keyword'\n" +
                "        and page['item'] is not null");//返回动态表对象
        //searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);


        //对搜索内容进行分词，并和原表的字段进行关联
        /*
        SELECT keyword,et
        FROM search_table,
        LATERAL TABLE(ik_analyze(full_word)) t(keyword)
*/
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et \n" +
                "        FROM search_table,\n" +
                "        LATERAL TABLE(ik_analyze(full_word)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
        //splitTable.execute().print();

        //分组、开窗、聚合计算
        /*
    SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') sst,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') cur_time,
    keyword,
    count(*)
    FROM TABLE(
    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))
    GROUP BY keyword,window_start, window_end;
         */
        Table keyCount = tableEnv.sqlQuery("SELECT\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    DATE_FORMAT(window_start, 'yyyyMMdd') cur_date,\n" +
                "    keyword,\n" +
                "    count(*) keyword_count\n" +
                "    FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "    GROUP BY keyword,window_start, window_end");

        tableEnv.createTemporaryView("key_count",keyCount);
        //print之后数据无法写入doris中
        //tableEnv.executeSql("select * from key_count").print();

        //将聚合结果写入到doris中
        /*
    CREATE TABLE flink_doris_sink (
    sst STRING,
    edt string,
    cur_date string,
    keyword string,
    key_count string
    )

         */
        tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                "    stt STRING,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                "    )" + SQLUtil.getDorisSinkDDL("dws_traffic_source_keyword_page_view_window"));

        //tableEnv.executeSql("insert into dws_traffic_source_keyword_page_view_window select * from key_count");

        keyCount.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}





























