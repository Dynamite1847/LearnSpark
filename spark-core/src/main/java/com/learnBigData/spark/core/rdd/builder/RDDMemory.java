package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;


public class RDDMemory {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .set("spark.master", "local[*]")
                .set("spark.app.name", "");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //创建RDD
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        //并行化集合，创建初始RDD
        //parallelize:并行, 只有scala才有makeRDD
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        numberRDD.collect().forEach(System.out::println);

        //停止环境
        sparkContext.stop();
    }
}
