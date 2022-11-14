package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDGroupByKey {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<String> numbers = Arrays.asList("a 1","a 2","a 3", "d 4");
        JavaRDD<String> numberRDD = sparkContext.parallelize(numbers,2);

        JavaPairRDD<String, Integer> pairs = numberRDD.mapToPair((PairFunction<String, String, Integer>) x -> new Tuple2(x.split(" ")[0], Integer.valueOf(x.split(" ")[1])));

        //根据指定规则对数据进行重新分区
        //Spark 默认的分区器是 HashPartitioner
//        如果partition类型相同，数量相同，分区器没有起作用，不然会生成新的RDD
//        pairs.partitionBy(new HashPartitioner(2)).saveAsTextFile("output");
        //partitioner分为rangePartitioner和HashPartitioner

        JavaPairRDD<String, Iterable<Integer>> reduceRDD= pairs.groupByKey();

        reduceRDD.collect().forEach(System.out::println);
    }
}
