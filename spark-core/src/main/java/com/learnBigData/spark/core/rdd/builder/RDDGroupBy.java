package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class RDDGroupBy {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers, 2);
        // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
        //相同的key值的数据会放置在一个组中

        JavaPairRDD<Integer, Iterable<Integer>> pairRDD = numberRDD.groupBy(x -> x % 2);
        pairRDD.collect().forEach(System.out::println);
    }
}
