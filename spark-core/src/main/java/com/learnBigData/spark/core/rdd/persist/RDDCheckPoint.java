package com.learnBigData.spark.core.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

public class RDDCheckPoint {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        //checkpoint需要落盘，需要指定检查点保存路径
        //检查点路径保存的文件，当作业执行完毕后，不会被删除
        //一般都保存在分布式存储路径中，如HDFS
        pairs.checkpoint();
        
        JavaPairRDD<String, Integer> reduceRDD = pairs.reduceByKey((x, y) -> x + y);

        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairs.groupByKey();

        reduceRDD.collect().forEach(System.out::println);

        groupRDD.collect().forEach(System.out::println);

        sparkContext.stop();
    }
}
