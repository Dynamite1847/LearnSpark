package com.learnBigData.spark.core.rdd.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class RDDSerializable {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> numbers = Arrays.asList("abnormal", "b", "c", "d");
        JavaRDD<String> numberRDD = sparkContext.parallelize(numbers);

        JavaRDD<String> match1 = Search.getMatch1("a", numberRDD);
        JavaRDD<String> match2 = Search.getMatch2("a", numberRDD);

        match1.collect().forEach(System.out::println);
        match2.collect().forEach(System.out::println);
        sparkContext.stop();

    }
}

class Search implements Serializable {
    public Search() {
    }

    public static Boolean isMatch(String query, String string) {
        return string.contains(query);
    }

    public static JavaRDD<String> getMatch1(String query, JavaRDD<String> rdd) {
        return rdd.filter(x -> isMatch(query, x));
    }

    public static JavaRDD<String> getMatch2(String query, JavaRDD<String> rdd) {
        return rdd.filter(x -> x.contains(query));
    }
}


