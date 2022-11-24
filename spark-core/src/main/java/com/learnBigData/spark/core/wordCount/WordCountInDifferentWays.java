package com.learnBigData.spark.core.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Option;
import scala.Tuple2;
import scala.collection.mutable.HashMap;

import java.util.Arrays;
import java.util.Map;

public class WordCountInDifferentWays {
    public static void main1(String[] args) {
//        建立spark链接
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);
//        执行业务操作

//        读取文件，以行为单位
        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();
        JavaRDD<String> lines1 = sparkContext.textFile("1.txt", 1).toJavaRDD();

//        拆分数据，形成单词,扁平化
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        根据单词分组
        JavaPairRDD<String, Iterable<String>> group = words.groupBy(word -> word);
        JavaPairRDD<String, Integer> wordCounts = group.mapValues(strings -> {
            int count = 0;
            for (String string : strings) {
                count++;
            }
            return count;
        });
        wordCounts.collect().forEach(System.out::println);
//        关闭链接
        sparkContext.stop();
    }

    public static void main2(String[] args) {
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
        JavaPairRDD<String, Iterable<Integer>> group = pairs.groupByKey();
        JavaPairRDD<String, Integer> wordCounts = group.mapValues(integers -> {
            int sum = 0;
            for (Integer integer : integers) {
                sum = sum + integer;
            }
            return sum;
        });
        wordCounts.collect().forEach(System.out::println);
    }

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

    public static void main4(String[] args) {
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
        JavaPairRDD<String, Integer> group = pairs.aggregateByKey(0, (x, y) -> x + y, (x, y) -> x + y);
        group.collect().forEach(System.out::println);
    }

    public static void main5(String[] args) {
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
        JavaPairRDD<String, Integer> group = pairs.foldByKey(0, (x, y) -> x + y);
        group.collect().forEach(System.out::println);
    }

    public static void main6(String[] args) {
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
        JavaPairRDD<String, Integer> group = pairs.combineByKey((Function<Integer, Integer>) x -> x, (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2, (Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer);
        group.collect().forEach(System.out::println);
    }

    public static void main7(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);
//        执行业务操作

//        读取文件，以行为单位
        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();

//        拆分数据，形成单词,扁平化
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        根据单词分组
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        Map<String, Long> resultMap = pairs.countByKey();

        System.out.println(resultMap);
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        SparkContext sparkContext = new SparkContext(sparkConf);
//        执行业务操作

//        读取文件，以行为单位
        JavaRDD<String> lines = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt", 1).toJavaRDD();

//        拆分数据，形成单词,扁平化
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
//        根据单词分组
        //TODO
        //用reduce实现该功能
    }

}
