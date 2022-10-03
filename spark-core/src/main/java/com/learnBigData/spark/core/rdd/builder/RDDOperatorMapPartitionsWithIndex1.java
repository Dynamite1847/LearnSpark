package com.learnBigData.spark.core.rdd.builder;

import com.google.common.collect.Iterators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RDDOperatorMapPartitionsWithIndex1 {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4);

        //并行化集合，创建初始RDD
        //parallelize:并行, 只有scala才有makeRDD
        JavaRDD<Integer> numberRDD = sparkContext.parallelize(numbers);

        JavaRDD<Integer> mapRDD = numberRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
                while (v2.hasNext()) {
                    System.out.println("index: " + v1 + " value: " + v2.next());
                }

                return Iterators.emptyIterator();

            }
        }, false);

        mapRDD.collect().forEach(System.out::println);
        sparkContext.stop();
    }
}
