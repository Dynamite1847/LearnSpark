package com.learnBigData.spark.core.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

public class RDDPersist {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //源码中cache和persist操作完全一样，persist可以将数据存储在其他位置
        pairs.cache();
        pairs.persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<String, Integer> reduceRDD = pairs.reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairs.groupByKey();

        reduceRDD.collect().forEach(System.out::println);

        groupRDD.collect().forEach(System.out::println);

        sparkContext.stop();
    }
}
