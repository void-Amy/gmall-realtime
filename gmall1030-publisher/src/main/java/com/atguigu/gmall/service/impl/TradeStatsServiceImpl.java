package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.beans.TradeProvinceOrderAmount;
import com.atguigu.gmall.mapper.TradeStatsMapper;
import com.atguigu.gmall.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceOrderAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
