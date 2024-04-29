package com.atguigu.gmall.mapper;

import com.atguigu.gmall.beans.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

@Mapper
public interface TradeStatsMapper {
    @Select("select sum(order_amount) total_amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window " +
            " PARTITION par#{date} GROUP BY province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);

}
