package com.learnBigData.spark.core.practice;

import java.io.Serializable;

public class UserVisitAction implements Serializable {
    private String date;

    public UserVisitAction(String date, String userId, String sessionId, Long pageId, String actionTime, String searchKeyword, Long clickCategoryId, Long clickProductId, String orderCategoryIds, String orderProductIds, String payCategoryIds, String payProductIds, Long cityId) {
        this.date = date;
        this.userId = userId;
        this.sessionId = sessionId;
        this.pageId = pageId;
        this.actionTime = actionTime;
        this.searchKeyword = searchKeyword;
        this.clickCategoryId = clickCategoryId;
        this.clickProductId = clickProductId;
        this.orderCategoryIds = orderCategoryIds;
        this.orderProductIds = orderProductIds;
        this.payCategoryIds = payCategoryIds;
        this.payProductIds = payProductIds;
        this.cityId = cityId;
    }

    private String userId;
    private String sessionId;
    private Long pageId;
    private String actionTime;
    //用户搜索的关键词
    private String searchKeyword;
    //某一个商品品类的 ID
    private Long clickCategoryId;
    //某一个商品的 ID
    private Long clickProductId;
    //一次订单中所有品类的ID集合
    private String orderCategoryIds;
    //一次订单中所有商品的ID集合
    private String orderProductIds;
    //一次支付中所有品类的 ID 集合
    private String payCategoryIds;
    //一次支付中所有商品的 ID 集合
    private String payProductIds;
    private Long cityId;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }

    public Long getCityId() {
        return cityId;
    }

    public void setCityId(Long cityId) {
        this.cityId = cityId;
    }
}
