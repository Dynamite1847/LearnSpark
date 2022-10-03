package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDOperatorTransformTest {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/apache.log");

        JavaRDD<String> mapRdd = rdd.map(line -> {
            List<String> strings = Arrays.asList(line.split(" "));
            return strings.get(6);
        });

        mapRdd.collect().forEach(System.out::println);
        sparkContext.stop();
    }
}
