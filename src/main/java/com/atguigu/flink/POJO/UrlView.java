package com.atguigu.flink.POJO;

/**
 * @author Adam-Ma
 * @date 2022/5/9 10:38
 * @Project my_flink_learning
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

import org.apache.commons.net.ntp.TimeStamp;

/**
 *      URL 对应的 POJO 类
 *          1、空参构造器
 *          2、public 的属性且非Final
 *          3、public 的类 且无非静态内部类
 */
public class UrlView {
    public String urlName;
    public Long visitCount;
    public Long startTime;
    public Long endTime;

    public UrlView() {
    }

    @Override
    public String toString() {
        return "UrlView{" +
                "urlName='" + urlName + '\'' +
                ", visitCount=" + visitCount +
                ", startTime=" + new TimeStamp(startTime) +
                ", endTime=" + new TimeStamp(endTime) +
                '}';
    }

    public String getUrlName() {
        return urlName;
    }

    public UrlView(String urlName, Long visitCount, Long startTime, Long endTime) {
        this.urlName = urlName;
        this.visitCount = visitCount;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public void setUrlName(String urlName) {
        this.urlName = urlName;
    }

    public Long getVisitCount() {
        return visitCount;
    }

    public void setVisitCount(Long visitCount) {
        this.visitCount = visitCount;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
}
