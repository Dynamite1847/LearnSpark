package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDMemoryParallelize {
    public static void main(String[] args){
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("RDD")
                .set("spark.default.parallelism","6");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //RDDd 分区，并行度
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        //parallelize:并行, 只有scala才有makeRDD
        //defaultParallelism(),默认并行度
        //"spark.default.parallelism", 从配置对象中获取配置参数，如果获取不到，这个属性就是当前环境的最大可用核数
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers,2);
        numberRDD.saveAsTextFile("output");

        //停止环境
        sparkContext.stop();
    }
}
