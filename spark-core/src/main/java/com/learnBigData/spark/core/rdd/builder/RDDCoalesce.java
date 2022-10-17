package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDCoalesce {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4,5,6);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers,2);

        // coalesce可以扩大分区，但是如果不进行shuffle操作，没有意义，不起作用
        //所以如果想要实现扩大分区的效果，需要使用shuffle操作
        //缩减分区：如果想要数据均衡，可以采用shuffle
        //扩大分区：repartition（底层就是coalesce）
        JavaRDD<Integer> newRDD = numberRDD.coalesce(3, true);
        newRDD.saveAsTextFile("output");
    }
}
