package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDDoubleValue {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 3, 4, 5, 2, 6);
        List<Integer> numbers1 = Arrays.asList(2, 4, 6, 8, 10, 12);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);
        JavaRDD<Integer> numberRDD1 = sparkContext.parallelize(numbers1);

        //交集,并集，差集要求数据类型保持一致
        JavaRDD<Integer> intersectionRDD = numberRDD.intersection(numberRDD1);
        intersectionRDD.collect().forEach(System.out::print);
        System.out.println();
        //并集
        JavaRDD<Integer> unionRDD = numberRDD.union(numberRDD1);
        unionRDD.collect().forEach(System.out::print);
        System.out.println();
        //差集
        JavaRDD<Integer> subtractRDD = numberRDD.subtract(numberRDD1);
        subtractRDD.collect().forEach(System.out::print);
        System.out.println();
        //拉链,两个数据源分区数量保持一致，分区数据数量要一致
        JavaPairRDD<Integer,Integer> zipRDD = numberRDD.zip(numberRDD1);
        zipRDD.collect().forEach(System.out::print);


    }
}
