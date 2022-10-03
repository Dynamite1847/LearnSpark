package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class RDDGroupBy1 {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<String> numbers = Arrays.asList("Hello", "Scala", "Spark", "Hadoop");
        JavaRDD<String> numberRDD = sparkContext.parallelize(numbers, 2);

        //分组和分区没有必然的关系
        JavaPairRDD<Object, Iterable<String>> pairRDD = numberRDD.groupBy(x -> x.charAt(0));
        pairRDD.collect().forEach(System.out::println);
    }
}
