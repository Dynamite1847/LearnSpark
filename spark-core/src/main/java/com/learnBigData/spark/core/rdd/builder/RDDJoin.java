package com.learnBigData.spark.core.rdd.builder;


import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class RDDJoin {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Integer> pairs = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("b", 2),
                        new Tuple2<String, Integer>("c", 3)),2);
        JavaPairRDD<String, Integer> pairs1 = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a", 4),
                        new Tuple2<String, Integer>("b", 5),
                        new Tuple2<String, Integer>("c", 6)),
                2);

        //join: 相同数据源的数据，想同的key的value会连接在一起形成元组
        //如果两个数据源中没有匹配上key，不会出现在结果中
        //如果有多个key相同，会依次匹配，可能会出现数据累量几何增长，导致性能降低
        JavaPairRDD<String, Tuple2<Integer,Integer>> reduceRDD = pairs.join(pairs1);
        reduceRDD.collect().forEach(System.out::println);
    }
}
