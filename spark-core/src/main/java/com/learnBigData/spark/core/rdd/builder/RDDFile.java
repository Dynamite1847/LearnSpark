package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDFile {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .set("spark.master", "local[*]")
                .set("spark.app.name", "");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //创建RDD
        //path既可以是目录也可以是文件名称
        //也可以使用通配符*
        JavaRDD<String> words = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt");
        JavaPairRDD<String, String> words1 = sparkContext.wholeTextFiles("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/");
        words.collect().forEach(System.out::println);

        //停止环境
        sparkContext.stop();
    }
}
