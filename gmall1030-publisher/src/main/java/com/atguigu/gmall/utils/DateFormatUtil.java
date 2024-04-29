package com.atguigu.gmall.utils;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatUtil {
    public static String getToday(){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        String formattedDate = formatter.format(date);
        return formattedDate;
    }

    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }

}
