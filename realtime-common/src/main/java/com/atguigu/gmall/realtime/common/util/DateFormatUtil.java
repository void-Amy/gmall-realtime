package com.atguigu.gmall.realtime.common.util;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

public class DateFormatUtil {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String tsToDate(Long ts){
        //TODO 注意：如果使用SimpleDateFormat,会存在线程安全问题，建议使用JDK1.8后日期包下的类
        /*//1.将 long 类型的时间转换为 Date 类型。
        Date date = new Date(ts);

        //2.将 Date 类型的数据转换为 Calendar 类型。
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        //使用 Calendar 类型的数据获取年、月、日的信息。
        *//*int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);*//*

        //将年、月、日的信息输出为 String 类型。
        String result = format.format(date);*/


        //不要上面那一对，直接这样子也是可以的：return format.format(ts);

        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    public static String tsToDateTime(Long ts){
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }

    public static String dateAdd(String time,int days){
        Date date = null;
        try {
            date = format.parse(time);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, days);
        date = calendar.getTime();
        String result = format.format(date);
        return result;

    }
}
