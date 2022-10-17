package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDSortBy {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 3, 4, 5, 2, 6);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers, 2);


        JavaRDD<Integer> newRDD = numberRDD.sortBy(x->x,true,2);
        newRDD.saveAsTextFile("output");
    }
}
