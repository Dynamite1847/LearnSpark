package com.learnBigData.spark.core.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

public class RDDPersist1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        pairs.cache();
        System.out.println(pairs.toDebugString());
        JavaPairRDD<String, Integer> reduceRDD = pairs.reduceByKey((x, y) -> x + y);

        reduceRDD.collect().forEach(System.out::println);
        System.out.println("***********************");
        System.out.println(reduceRDD.toDebugString());

        sparkContext.stop();
    }
}
