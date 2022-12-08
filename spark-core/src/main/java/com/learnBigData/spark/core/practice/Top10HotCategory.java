package com.learnBigData.spark.core.practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Top10HotCategory {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Top10HotCategory");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //1.读取原始日志数据
        JavaRDD<String> userActionRDD = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user_visit_action.txt");
        //2.统计品类点击数量
        JavaRDD<String> clickActionRDD = userActionRDD.filter(action -> {
            String[] splitData = action.split("_");
            return Integer.valueOf(splitData[6]) != -1;
        });
        JavaPairRDD<String, Integer> clickActionPair = clickActionRDD.mapToPair(action -> {
            String[] splitData = action.split("_");
            return new Tuple2<>(splitData[6], 1);
        }).reduceByKey((x, y) -> x + y);
//        clickActionPair.collect().forEach(System.out::println);
//        System.out.println("=========================");
        //3.统计品类下单数量
        JavaRDD<String> placeOrderRDD = userActionRDD.filter(action -> {
            String[] splitData = action.split("_");
            Boolean flag = !splitData[8].equals("null");
            return !splitData[8].equals("null");
        });

        JavaRDD<String> orderCountRDD = placeOrderRDD.flatMap(action -> {
            String[] splitData = action.split("_");
            String[] splitItems = splitData[8].split(",");
            return Arrays.stream(splitItems).iterator();
        });

        JavaPairRDD<String, Integer> orderPairRDD = orderCountRDD.mapToPair(order -> new Tuple2<>(order, 1)).reduceByKey((x, y) -> x + y);
//        orderPairRDD.collect().forEach(System.out::println);
//        System.out.println("===========================");
        //4.统计品类支付数量
        JavaRDD<String> payRDD = userActionRDD.filter(action -> {
            String[] splitData = action.split("_");
            return !splitData[10].equals("null");
        });
        JavaRDD<String> payCountRDD = payRDD.flatMap(action -> {
            String[] splitData = action.split("_");
            String[] splitItems = splitData[10].split(",");
            return Arrays.stream(splitItems).iterator();
        });
        JavaPairRDD<String, Integer> payPairRDD = payCountRDD.mapToPair(pay -> new Tuple2<>(pay, 1)).reduceByKey((x, y) -> x + y);
//        payPairRDD.collect().forEach(System.out::println);
//        System.out.println("===========================");
        //5.根据数量进行排序，并取前十名
        //点击数量排序，下单数量排序，支付数量排序
        //元组排序，先比较第一个，再比较第二个，再比较第三个，以此类推
        //（品类id，（点击数量，下单数量，支付数量））
        //cogroup可能存在shuffle

        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> clickActionResult = clickActionPair.map((cid) -> new Tuple2<>(cid._1(), new Tuple3<>(cid._2, 0, 0)));
        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> orderActionResult = orderPairRDD.map((orderId) -> new Tuple2<>(orderId._1(), new Tuple3<>(0, orderId._2, 0)));
        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> payActionResult = payPairRDD.map(payID -> new Tuple2<>(payID._1(), new Tuple3<>(0, 0, payID._2)));
        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> unionRDD = clickActionResult.union(orderActionResult).union(payActionResult);
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> unionPairRDD = unionRDD.mapToPair(x -> x);
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> resultPairRDD = unionPairRDD.reduceByKey((t1, t2) -> new Tuple3<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3()));

//        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> cogroupRDD = clickActionPair.cogroup(orderPairRDD, payPairRDD);
//        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> analysisPairRDD = cogroupRDD.mapValues(x -> {
//            Integer clickCount = 0;
//            Iterator<Integer> clickIterator = x._1().iterator();
//            if (clickIterator.hasNext()) {
//                clickCount = (Integer) clickIterator.next();
//            }
//            Integer orderCount = 0;
//            Iterator<Integer> orderIterator = x._2().iterator();
//            if (orderIterator.hasNext()) {
//                orderCount = (Integer) orderIterator.next();
//            }
//            Integer payCount = 0;
//            Iterator<Integer> payIterator = x._3().iterator();
//            if (payIterator.hasNext()) {
//                payCount = (Integer) payIterator.next();
//            }
//
//            return new Tuple3<>(clickCount, orderCount, payCount);
//        });

        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> analysisRDD = resultPairRDD.map(x -> x);

        List<Tuple2<String, Tuple3<Integer, Integer, Integer>>> results = analysisRDD.sortBy(x -> x._2()._1(), false, 2).take(10);

        for (Tuple2<String, Tuple3<Integer, Integer, Integer>> result : results) {
            System.out.println(result);
        }
        //6.将结果打印到控制台
        sparkContext.stop();
    }
}
