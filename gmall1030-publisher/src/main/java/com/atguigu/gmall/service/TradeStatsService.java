package com.atguigu.gmall.service;

import com.atguigu.gmall.beans.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

public interface TradeStatsService {
    BigDecimal getGMV(Integer date);

    List<TradeProvinceOrderAmount> getProvinceOrderAmount(Integer date);
}
