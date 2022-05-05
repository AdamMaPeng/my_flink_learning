package com.atguigu.flink.utils;

import java.util.ResourceBundle;

/**
 * @author Adam-Ma
 * @date 2022/5/3 10:45
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */
public class ConfigUtils {
    // 1、提供 public static 的方法，供外部直接调用
    public static String getConfig(String key){
        // 1、获取 配置对象
        ResourceBundle bundle = ResourceBundle.getBundle("conf");

        // 2、通过 配置对象读取外部 key
        String value = bundle.getString(key);
        return value;
    }

    public static void main(String[] args) {
        System.out.println(ConfigUtils.getConfig("hostName"));
    }
}
