package com.atguigu.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/*
分词方法
 */
public class IkUtil {
    public static Set<String> split(String s) {
        Set<String> result = new HashSet<>();
        // String => Reader

        Reader reader = new StringReader(s);
        // 智能分词
        // max_word
        //new 一个分词器
        //useSmart – 为true，使用智能分词策略 非智能分词：细粒度输出所有可能的切分结果
        // 智能分词： 合并数词和量词，对分词结果进行歧义判断
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();//分词器内的指针，类似迭代器的东西
            while (next != null) {
                String word = next.getLexemeText();//获取分词的内容
                result.add(word);//把分词放入集合中
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        return result;
    }

    public static void main(String[] args) {
        Set<String> split = split("5G折叠屏手机");
        System.out.println(split);
    }
}
