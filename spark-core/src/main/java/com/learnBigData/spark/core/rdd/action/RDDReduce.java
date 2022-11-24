package com.learnBigData.spark.core.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RDDReduce {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        List<Integer> numbers = Arrays.asList(4,2,3,1);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers, 2);

        Integer reduce = numberRDD.reduce((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        //collect:方法会将不同分区的数据按照分区顺序采集到Driver端内存中进行处理，形成数组
        List<Integer> collect = numberRDD.collect();
        System.out.println(collect);

        numberRDD.first();
        numberRDD.take(3);

        List<Integer> integers = numberRDD.takeOrdered(3);

        System.out.println(integers);

        //初始值不仅会参与分区内计算，还会参与分区间计算
        Integer aggregate = numberRDD.aggregate(10, (x, y) -> x + y, (x, y) -> x + y);
        System.out.println(aggregate);


        Map<Integer, Long> integerLongMap = numberRDD.countByValue();
        System.out.println(integerLongMap);
    }
}
