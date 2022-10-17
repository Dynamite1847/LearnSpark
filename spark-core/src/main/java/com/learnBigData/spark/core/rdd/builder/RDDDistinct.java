package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDDistinct {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 1, 2, 3, 4);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        /*
        底层用了reduceByKey
        */
        numberRDD.distinct()
                .collect()
                .forEach(System.out::println);
    }
}
