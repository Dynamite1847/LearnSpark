package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDFilePar1 {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("RDD");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //1.数据以行为单位,和行没关系
        //2.数据读取 以偏移量为单位
        //数据分区的偏移量范围的计算
        /*
            0 => [0, 3]
            1 => [3, 6]
            2 => [6, 7]
        */
        //14/7 = 2分区
        /*
        1234567@@ => 0123456
        89@@      =>9101112
        0         =>13
        按偏移量分区
        [0, 7] =>1234567
        [7,14] =>890
         */

        //如果数据源有多个文件，以文件为分区

        JavaRDD<String> words = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/words.txt", 2);

        words.saveAsTextFile("output");

        //停止环境
        sparkContext.stop();
    }
}
