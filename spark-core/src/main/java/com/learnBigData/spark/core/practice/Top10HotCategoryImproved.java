package com.learnBigData.spark.core.practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Top10HotCategoryImproved {
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
        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> resultRDD = userActionRDD.flatMap((FlatMapFunction<String, Tuple2<String, Tuple3<Integer, Integer, Integer>>>) s -> {
            String[] splitData = s.split("_");
            if (Integer.parseInt(splitData[6]) != -1) {
                String[] splitItems = new String[1];
                splitItems[0]= splitData[6];
                return Arrays.stream(splitItems).map(id -> new Tuple2<>(id, new Tuple3<>(1, 0, 0))).iterator();
            } else if (!splitData[8].equals("null")) {
                String[] splitItems = splitData[8].split(",");
                return Arrays.stream(splitItems).map(id -> new Tuple2<>(id, new Tuple3<>(0, 1, 0))).iterator();
            } else if (!splitData[10].equals("null")) {
                String[] splitItems = splitData[10].split(",");
                return Arrays.stream(splitItems).map(id -> new Tuple2<>(id, new Tuple3<>(0, 0, 1))).iterator();
            }
            return new Iterator<Tuple2<String, Tuple3<Integer, Integer, Integer>>>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Tuple2<String, Tuple3<Integer, Integer, Integer>> next() {
                    return null;
                }
            };
        });

        //5.根据数量进行排序，并取前十名
        //点击数量排序，下单数量排序，支付数量排序
        //元组排序，先比较第一个，再比较第二个，再比较第三个，以此类推
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> unionPairRDD = resultRDD.mapToPair(x -> x);
        //（品类id，（点击数量，下单数量，支付数量））
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> resultPairRDD = unionPairRDD.reduceByKey((t1, t2) -> new Tuple3<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3()));

        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> analysisRDD = resultPairRDD.map(x -> x);

        analysisRDD.collect().forEach(System.out::println);

        List<Tuple2<String, Tuple3<Integer, Integer, Integer>>> results = analysisRDD.sortBy(x -> x._2()._1(), false, 2).take(10);

        for (Tuple2<String, Tuple3<Integer, Integer, Integer>> result : results) {
            System.out.println(result);
        }
        //6.将结果打印到控制台
        sparkContext.stop();
    }



}
