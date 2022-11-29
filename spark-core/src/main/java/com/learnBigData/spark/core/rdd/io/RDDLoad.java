package com.learnBigData.spark.core.rdd.io;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RDDLoad {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> pairs = sparkContext.parallelizePairs(Lists.newArrayList(
                new Tuple2<>("nba", "xxxxx"),
                new Tuple2<>("wnba", "xxxxx"),
                new Tuple2<>("cba", "xxxxx"),
                new Tuple2<>("nba", "xxxxx")), 3);

    }


}
