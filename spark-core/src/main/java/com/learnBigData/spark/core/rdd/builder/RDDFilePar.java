package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDFilePar {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster( "local[*]")
                .setAppName("RDD");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //创建RDD
        //partition为最小分区值， miniPartition是最小的分区数量
        //文件的partition底层就是用hadoop的读取方式
        //分区数量计算方式： totalSize = 7
        //                goalSize = 7 / 2 = 3 (byte)
        //判断剩余数量是否小于10%，如果大于创建新的分区
        JavaRDD<String> words = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/1.txt",2);
//        JavaPairRDD<String, String> words1 = sparkContext.wholeTextFiles("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/");
        words.saveAsTextFile("output" );

        //停止环境
        sparkContext.stop();
    }
}
