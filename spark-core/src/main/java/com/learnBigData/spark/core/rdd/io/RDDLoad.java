package com.learnBigData.spark.core.rdd.io;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RDDLoad {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/output");
        rdd.collect().forEach(System.out::println);
        JavaRDD<Object> rdd1 = sparkContext.objectFile("/Users/dongyu/IdeaProjects/LearnSpark/output1");
        rdd1.collect().forEach(System.out::println);
    }
}
