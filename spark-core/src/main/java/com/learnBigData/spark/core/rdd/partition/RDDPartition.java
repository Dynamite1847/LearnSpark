package com.learnBigData.spark.core.rdd.partition;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class RDDPartition {
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
        JavaPairRDD<String, String> partitionRDD = pairs.partitionBy(new MyPartitioner());
        partitionRDD.saveAsTextFile("/Users/dongyu/IdeaProjects/LearnSpark/output");
        partitionRDD.saveAsObjectFile("/Users/dongyu/IdeaProjects/LearnSpark/output1");

    }

    static class MyPartitioner extends Partitioner {

        @Override
        public int numPartitions() {
            return 3;
        }

        @Override
        public int getPartition(Object key) {
            switch (key.toString()) {
                case "nba":
                    return 0;
                case "wnba":
                    return 1;
                case "cba":
                    return 2;
            }
            return 2;
        }
    }
}
