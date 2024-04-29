import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DorisTest {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                "    stt STRING,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                "    )" + SQLUtil.getDorisSinkDDL("dws_traffic_source_keyword_page_view_window"));


        tableEnv.executeSql("insert into dws_traffic_source_keyword_page_view_window values(" +
                "'2024-04-24 21:03:50', '2024-04-24 21:04:00', '2024-04-24','  数据',1)");
    }
}
