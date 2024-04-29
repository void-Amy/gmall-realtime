package com.atguigu.gmall.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.beans.TradeProvinceOrderAmount;
import com.atguigu.gmall.service.TradeStatsService;
import com.atguigu.gmall.utils.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class TradeStatsController {

    @Autowired
    private TradeStatsService tradeStatsService;

    /**
     * http://localhost:8070/total
     * @param date
     * @return
     */
    @GetMapping("/total")
    public String getTotalAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("status",0);
        jsonObj.put("data",gmv);
        return jsonObj.toJSONString();
    }

    @GetMapping("/province")
    public Map getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceOrderAmount(date);

        Map resMap = new HashMap();
        resMap.put("status",0);
        Map dataMap = new HashMap();
        List dataList = new ArrayList();
        for (TradeProvinceOrderAmount provinceOrderAmount : provinceOrderAmountList) {
            Map map = new HashMap();
            map.put("name",provinceOrderAmount.getProvinceName());
            map.put("value",provinceOrderAmount.getOrderAmount());
            dataList.add(map);
        }
        dataMap.put("mapData",dataList);
        dataMap.put("valueName","交易额");
        resMap.put("data",dataMap);
        return resMap;

    }


}
