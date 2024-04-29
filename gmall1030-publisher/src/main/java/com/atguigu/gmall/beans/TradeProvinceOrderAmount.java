package com.atguigu.gmall.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
