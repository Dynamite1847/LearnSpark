package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

public class RDDGlom {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers,2);
        JavaRDD<List<Integer>> glomRDD= numberRDD.glom();
        glomRDD.collect().forEach(System.out::println);
        JavaRDD<Integer> maxRDD =  glomRDD.map(list -> Collections.max(list));
        Integer sum   = maxRDD.reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        System.out.println(sum);
    }
}
