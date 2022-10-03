package com.learnBigData.spark.core.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {
    public static void main(String[] args){
//        建立spark链接
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);
//        执行业务操作

//        读取文件，以行为单位
        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt",1).toJavaRDD();
        JavaRDD<String> lines1 = sparkContext.textFile("1.txt",1).toJavaRDD();

//        拆分数据，形成单词,扁平化
        JavaRDD<String> words = lines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
//        根据单词分组
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
//         相同的key的数据，可以对value进行reduce聚合
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((v1, v2) -> v1 + v2)
                .repartition(1);
//         输出目录
        final List<Tuple2<String, Integer>> output = wordCounts.collect();
        for (Tuple2 tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

//        关闭链接
        sparkContext.stop();
    }
}
