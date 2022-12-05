package com.learnBigData.spark.core.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;


import java.util.Arrays;
import java.util.List;

public class RDDAccumulator1 {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1,2,3,4);
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers, 2);

        //获取系统累加器
        //spark默认提供简单数据聚合的累加器
        // 内置的累加器有三种，LongAccumulator、DoubleAccumulator、CollectionAccumulator

        LongAccumulator longAccumulator = new LongAccumulator();


        //注册累加器
        sparkContext.sc().register(longAccumulator,"longAccumulator");

        //使用累加器
        JavaRDD<Object> result = numberRDD.map(new Function<Integer, Object>() {
            @Override
            public Object call(Integer integer) throws Exception {
                longAccumulator.add(integer);
                return longAccumulator.value();
            }
        });

        result.collect();
        result.collect();
        System.out.println(longAccumulator.value());;
    }
}
