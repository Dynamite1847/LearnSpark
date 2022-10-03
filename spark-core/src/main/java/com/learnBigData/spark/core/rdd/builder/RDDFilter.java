package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RDDFilter {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers,2);

        JavaRDD<Integer> oddRDD = numberRDD.filter(i -> {
            return i % 2 != 0;
        });
        oddRDD.collect().forEach(System.out::println);
    }
}
