package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Int;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RDDOperatorMapPartitions {
    public static void main(String[] args){
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);

        //并行化集合，创建初始RDD
        //parallelize:并行, 只有scala才有makeRDD
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers,2);

        JavaRDD<Integer> mpRDD= numberRDD.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) iterator -> {
            System.out.println("<<<<<<>>>>>>");
            List<Integer> list = new ArrayList<>();
            Integer max = new Integer(0);
            while (iterator.hasNext()){
                Integer next = iterator.next();
                if (max<next){
                    max = next;
                }
            }
            list.add(max);
            return list.iterator();
        });

        mpRDD.collect().forEach(System.out::println);
        sparkContext.stop();
    }
}
