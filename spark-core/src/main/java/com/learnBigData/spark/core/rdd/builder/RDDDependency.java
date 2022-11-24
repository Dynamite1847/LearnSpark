package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class RDDDependency {
    public static void main3(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);
//        执行业务操作

//        读取文件，以行为单位
        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();
        JavaRDD<String> lines1 = sparkContext.textFile("1.txt", 1).toJavaRDD();

//        拆分数据，形成单词,扁平化
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        根据单词分组
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> group = pairs.reduceByKey((x, y) -> x + y);
        group.collect().forEach(System.out::println);
    }

}
