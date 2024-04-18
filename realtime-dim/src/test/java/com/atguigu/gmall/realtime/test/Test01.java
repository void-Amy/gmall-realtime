package com.atguigu.gmall.realtime.test;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

public class Test01 {
    @Test
    public void test01(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("user_id",111);
        jsonObject.put("user_Last_name","zhangsan");

        User user = jsonObject.toJavaObject(User.class);
        System.out.println(user);
    }
}

class User {
    private int userId;
    private String userLastName;

    @Override
    public String toString() {
        return "User{" +
                "userId=" + userId +
                ", userLastName='" + userLastName + '\'' +
                '}';
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getUserLastName() {
        return userLastName;
    }

    public void setUserLastName(String userLastName) {
        this.userLastName = userLastName;
    }
}
