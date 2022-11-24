package com.learnBigData.spark.core.rdd.action;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class RDDSave implements Serializable {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);
        //foreach是Driver集合内存的循环遍历方法
        numberRDD.collect().forEach(System.out::println);
        System.out.println("*************");
        //foreach是Executor端内存数据读取（分布），直接在executor端打印
        numberRDD.foreach(x-> System.out.println(x));
        sparkContext.stop();

    }
}
