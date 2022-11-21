package com.learnBigData.spark.core.rdd.builder;


import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

public class RDDLeftOuterJoin {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Integer> pairs = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 2),
                        new Tuple2<String, Integer>("c", 3)),2);
        JavaPairRDD<String, Integer> pairs1 = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a", 4),
                        new Tuple2<String, Integer>("b", 5)),
                2);

        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> reduceRDD = pairs.leftOuterJoin(pairs1);
        reduceRDD.collect().forEach(System.out::println);
    }
}
