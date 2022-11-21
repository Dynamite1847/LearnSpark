package com.learnBigData.spark.core.rdd.builder;


import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class RDDFoldByKey {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Integer> pairs = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("a", 2),
                        new Tuple2<String, Integer>("b", 3),
                        new Tuple2<String, Integer>("b", 4),
                        new Tuple2<String, Integer>("b", 5),
                        new Tuple2<String, Integer>("a", 6)),
                2);

        //如果分区内分区间计算逻辑相同可以使用foldByKey
        JavaPairRDD<String, Integer> reduceRDD = pairs.foldByKey(0,  (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        reduceRDD.collect().forEach(System.out::println);
    }
}
