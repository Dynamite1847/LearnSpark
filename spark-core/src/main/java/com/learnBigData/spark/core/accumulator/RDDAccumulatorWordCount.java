package com.learnBigData.spark.core.accumulator;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RDDAccumulatorWordCount {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> words = Arrays.asList("Hello", "Spark", "Hello", "Hadoop");
        JavaRDD<String> wordsRDD = sparkContext.parallelize(words, 2);

        MyAccumulator myAccumulator = new MyAccumulator();
        sparkContext.sc().register(myAccumulator, "wordCountAcc");

        //使用累加器
        wordsRDD.foreach(word -> myAccumulator.add(word));
        System.out.println(myAccumulator.value());
    }

    //继承AccumulatorV2
    //IN：输入类型
    //OUT：输出类型
    static class MyAccumulator extends AccumulatorV2<String, HashMap<String, Integer>> {

        private HashMap map = new HashMap<String, Integer>();

        //判断是否为初始状态
        @Override
        public boolean isZero() {
            return map.isEmpty();
        }

        @Override
        public AccumulatorV2<String, HashMap<String, Integer>> copy() {
            return new MyAccumulator();
        }

        @Override
        public void reset() {
            map.clear();
        }

        @Override
        public void add(String word) {
            Integer newCount = (Integer) map.getOrDefault(word, 0);
            newCount += 1;
            map.put(word, newCount);
        }

        @Override
        public void merge(AccumulatorV2<String, HashMap<String, Integer>> other) {
            HashMap map1 = this.map;
            HashMap map2 = other.value();

            map2.forEach((word, count) -> {
                Integer i = (Integer) map1.getOrDefault(word, 0);
                i += (Integer) count;
                map1.put(word, i);
            });
        }

        @Override
        public HashMap<String, Integer> value() {
            return map;
        }


    }
}

