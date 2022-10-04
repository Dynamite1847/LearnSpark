package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDDSample {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        /*
        sample算子需要传递三个参数
        第一个参数表示，抽取数据后是否将数据放回
        第二个参数表示，数据源中每条数据被抽取的概率
        第二个参数设置了一个基准值，
        第三个参数表示，抽取随机数据时随机算法的种子
        如果不传递第三个参数，使用的是系统时间
        */
        numberRDD.sample(false, 0.4, 1)
                .collect()
                .forEach(System.out::println);
    }
}
