package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDPParallelCalculate {
    public static void main(String[] args){
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

        //并行化集合，创建初始RDD
        //parallelize:并行, 只有scala才有makeRDD
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        JavaRDD<Integer> rdd= numberRDD.map(i -> {
            System.out.println("<<<<<<" + i);
            return i;
        });

        JavaRDD<Integer> rdd1= rdd.map(i -> {
            System.out.println("######" + i);
            return i;
        });

        rdd1.collect();
        sparkContext.stop();
    }
}
