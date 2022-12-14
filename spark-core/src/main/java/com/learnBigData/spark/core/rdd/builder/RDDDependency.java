package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;

public class RDDDependency {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);
//        执行业务操作

//        读取文件，以行为单位
        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();
        System.out.println(lines.toDebugString());
        System.out.println("**********************");
//        拆分数据，形成单词,扁平化
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println(words.toDebugString());
        System.out.println("**********************");
//        根据单词分组
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        System.out.println(pairs.toDebugString());
        System.out.println("**********************");
        JavaPairRDD<String, Integer> group = pairs.reduceByKey((x, y) -> x + y);
        System.out.println(group.toDebugString());
        group.collect().forEach(System.out::println);
    }

}
