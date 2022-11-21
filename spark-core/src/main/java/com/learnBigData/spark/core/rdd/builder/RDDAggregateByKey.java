package com.learnBigData.spark.core.rdd.builder;


import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDAggregateByKey {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String,Integer> pairs = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a",1),
                        new Tuple2<String, Integer>("a",2),
                        new Tuple2<String, Integer>("a",3),
                        new Tuple2<String, Integer>("a",4)),
                2);


        //aggregateByKey 存在参数柯里化
        //有三个参数，第一个参数用于碰见第一个key时进行分区计算
        //第二个参数是函数分区内的计算规则
        //第三个参数是分区间计算规则

        JavaPairRDD<String, Integer> reduceRDD= pairs.aggregateByKey(0, (Function2<Integer, Integer, Integer>) (v1, v2) -> Math.max(v1,v2), (Function2<Integer, Integer, Integer>) (v1, v2) -> v1+v2);

        reduceRDD.collect().forEach(System.out::println);
    }
}
