package com.learnBigData.spark.core.accumulator;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.HashMap;

public class Broadcast {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String,Integer> pairs = sparkContext.parallelizePairs(Lists.newArrayList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("b", 2),
                        new Tuple2<>("c", 3)),
                2);

        JavaPairRDD<String,Integer> pairs1 = sparkContext.parallelizePairs(Lists.newArrayList(
                        new Tuple2<>("a", 4),
                        new Tuple2<>("b", 5),
                        new Tuple2<>("c", 6)),
                2);

        org.apache.spark.broadcast.Broadcast<JavaPairRDD<String, Integer>> broadcast = sparkContext.broadcast(pairs1);
        //join会导致数据几何增长，影响shuffle性能，不推荐使用
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = pairs.join(broadcast.value());


        joinRDD.collect().forEach(System.out::println);
    }

}
