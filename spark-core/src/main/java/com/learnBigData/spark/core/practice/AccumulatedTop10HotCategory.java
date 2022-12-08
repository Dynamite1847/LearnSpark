package com.learnBigData.spark.core.practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;


public class AccumulatedTop10HotCategory {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Top10HotCategory");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //优化方向：存在大量的shuffle操作（reduceByKey）
        //reduceByKey聚合算子，spark会提供优化，有缓存操作，不需要重复计算

        //1.读取原始日志数据
        JavaRDD<String> userActionRDD = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user_visit_action.txt");
        //2.转换数据结构
        Top10HotCategoryAccumulator top10HotCategoryAccumulator = new Top10HotCategoryAccumulator();
        sparkContext.sc().register(top10HotCategoryAccumulator, "hotCategory");
        userActionRDD.foreach(userAction -> {
            String[] splitData = userAction.split("_");
            if (Integer.parseInt(splitData[6]) != -1) {
                top10HotCategoryAccumulator.add(new Tuple2<>(splitData[6], "click"));
            } else if (!splitData[8].equals("null")) {
                String[] splitItems = splitData[8].split(",");
                for (String splitItem : splitItems) {
                    top10HotCategoryAccumulator.add(new Tuple2<>(splitItem, "order"));
                }
            } else if (!splitData[10].equals("null")) {
                String[] splitItems = splitData[10].split(",");
                for (String splitItem : splitItems) {
                    top10HotCategoryAccumulator.add(new Tuple2<>(splitItem, "pay"));
                }
            }
        });

        HashMap<String, HotCategory> resultMap = top10HotCategoryAccumulator.value();

        List<HashMap.Entry<String,HotCategory>> list = new ArrayList<HashMap.Entry<String,HotCategory>>(resultMap.entrySet());
        //然后通过比较器来实现排序
        Collections.sort(list,new Comparator<HashMap.Entry<String,HotCategory>>() {
            //升序排序
            public int compare(HashMap.Entry<String, HotCategory> o1,
                               HashMap.Entry<String, HotCategory> o2) {
                return o2.getValue().clickCount.compareTo(o1.getValue().clickCount);
            }
        });

        for(Map.Entry<String,HotCategory> mapping:list){
            System.out.println(mapping.getKey()+":"+mapping.getValue());
        }

        //6.将结果打印到控制台
        sparkContext.stop();
    }
}




