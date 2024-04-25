package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.dws.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * UDTF,一个输入，多个输出
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String kw) {
        if (kw == null) {
            return;
        }
        // "华为手机白色手机"
        Set<String> keywords = IkUtil.split(kw);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }

}
