package com.learnBigData.spark.core.practice;

import java.io.Serializable;

public class HotCategory implements Serializable {

    Integer clickCount;
    Integer orderCount;
    Integer payCount;

    public HotCategory(Integer clickCount, Integer orderCount, Integer payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    @Override
    public String toString() {
        return "HotCategory{" +
                "clickCount=" + clickCount +
                ", orderCount=" + orderCount +
                ", payCount=" + payCount +
                '}';
    }
}

