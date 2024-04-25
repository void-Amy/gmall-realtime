import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 */

public class Flink02_Demo {
    public static void main(String[] args) {
        //基本环境准备
        //指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);



        //指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //检查点相关色湖之

        //从kafka first主题中读取员工数据
        TableResult emp = tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  `empno` integer,\n" +
                "  `ename` string,\n" +
                "  `deptno` integer\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tableEnv.executeSql("select * from emp").print();

        //从hbase表中读取部门数据


        //将员工和部门进行关联
        //
        //应该使用lookUp join，它的底层实现原理和普通的内外连接完全不一样，底层不会维护状态
        //它是以左表进行驱动的，当左表数据到来的时候，发送请求和右表进行关联

        //将关联结果写到kafka主题中


    }

}
