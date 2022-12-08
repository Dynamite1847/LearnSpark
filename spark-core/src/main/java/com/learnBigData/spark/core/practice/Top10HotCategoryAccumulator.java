package com.learnBigData.spark.core.practice;

import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.HashMap;

public class Top10HotCategoryAccumulator extends AccumulatorV2<Tuple2<String, String>, HashMap<String, HotCategory>> {
    private HashMap<String, HotCategory> hotCategoryMap = new HashMap();

    @Override
    public boolean isZero() {
        return hotCategoryMap.isEmpty();
    }

    @Override
    public AccumulatorV2<Tuple2<String, String>, HashMap<String, HotCategory>> copy() {
        return new Top10HotCategoryAccumulator();
    }

    @Override
    public void reset() {
        hotCategoryMap.clear();
    }

    @Override
    public void add(Tuple2<String, String> v) {
        String cid = v._1;
        String actionType = v._2;
        HotCategory hotCategory = hotCategoryMap.getOrDefault(cid, new HotCategory(0, 0, 0));
        if (actionType.equals("click")) {
            hotCategory.clickCount += 1;
        } else if (actionType.equals("pay")) {
            hotCategory.orderCount += 1;
        } else if (actionType.equals("order")) {
            hotCategory.payCount += 1;
        }
        hotCategoryMap.put(cid, hotCategory);
    }

    @Override
    public void merge(AccumulatorV2<Tuple2<String, String>, HashMap<String, HotCategory>> other) {
        HashMap<String, HotCategory> map1 = this.hotCategoryMap;
        HashMap<String, HotCategory> map2 = other.value();
        map2.forEach((k, v) -> {
            HotCategory category = map1.getOrDefault(k, new HotCategory(0, 0, 0));
            category.clickCount += v.clickCount;
            category.orderCount += v.orderCount;
            category.payCount += v.payCount;
            map1.put(k, category);
        });
    }

    @Override
    public HashMap<String, HotCategory> value() {
        return hotCategoryMap;
    }
}



