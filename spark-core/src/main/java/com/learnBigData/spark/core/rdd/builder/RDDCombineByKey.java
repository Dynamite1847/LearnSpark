package com.learnBigData.spark.core.rdd.builder;


import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class RDDCombineByKey {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Integer> pairs = sparkContext.parallelizePairs(Lists.<Tuple2<String, Integer>>newArrayList(
                        new Tuple2<String, Integer>("a", 1),
                        new Tuple2<String, Integer>("a", 2),
                        new Tuple2<String, Integer>("b", 3),
                        new Tuple2<String, Integer>("b", 4),
                        new Tuple2<String, Integer>("b", 5),
                        new Tuple2<String, Integer>("a", 6)),
                2);

        //三个参数
        //第一个参数：将相同key的第一个数据进行结构转换，实现操作
        //第二个参数：分区内计算规则
        //第三个参数：分区间的计算规则
        JavaPairRDD<String, Tuple2<Integer,Integer>> reduceRDD = pairs.combineByKey((Function<Integer, Tuple2<Integer, Integer>>) x -> new Tuple2<Integer, Integer>(x, 1), (Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>) (v, x) -> new Tuple2<Integer, Integer>(v._1() + x, v._2() + 1),
                (Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (v, v1) -> new Tuple2<Integer, Integer>(v._1() + v1._1(), v._2() + v1._2()));
        reduceRDD.collect().forEach(System.out::println);
    }
}
