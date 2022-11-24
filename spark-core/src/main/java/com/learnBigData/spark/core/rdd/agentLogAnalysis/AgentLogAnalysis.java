package com.learnBigData.spark.core.rdd.agentLogAnalysis;

import org.apache.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class AgentLogAnalysis {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaRDD<String> sourceData;
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            //1. 获取原始数据：时间戳，省份，城市，用户，广告
            sourceData = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/agent.log", 1);
            //2. 将原始数据进行结构的转换，方便统计时间戳，省份，城市，用户，广告 =>（（省份，广告），1）
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> extractedData = sourceData.mapToPair(line -> {
                List<String> strings = Arrays.asList(line.split(" "));
                return new Tuple2<>(new Tuple2<>(Integer.valueOf(strings.get(1)), Integer.valueOf(strings.get(4))), 1);
            });
            //3.将转换结构后的数据进行分组聚合（（省份，广告），1）=>（（省份，广告），sum）
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> sumData = extractedData.reduceByKey(Integer::sum);
            //4.将聚合结果进行结构的转换（（省份，广告），sum）=> （省份，（广告，sum））
            JavaPairRDD<Integer, Tuple2<Integer, Integer>> aggregatedData = sumData.mapToPair(line -> new Tuple2<>(line._1._1, new Tuple2<>(line._1._2, line._2)));
            //5.将转换结构后的数据根据省份进行分组（省份，【广告，sumA），（广告，sumB）】）
            JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> groupedData = aggregatedData.groupByKey();
            //6.将分组后的数据进行组内排序（desc）,取前三名
            JavaPairRDD<Integer, List<Tuple2<Integer, Integer>>> sortedData = groupedData.mapValues(iter -> {
                List<Tuple2<Integer, Integer>> valueList = Lists.newArrayList(iter);
                valueList.sort(new Compare());
                return new ArrayList<>(valueList.subList(0, 3));
            });
            List<Tuple2<Integer, List<Tuple2<Integer, Integer>>>> results = sortedData.collect();
            //7.打印结果
            for (Tuple2<Integer, List<Tuple2<Integer, Integer>>> temp : results) {
                System.out.println(temp);
            }
        }
    }


    private static class Compare implements Comparator<Tuple2<Integer, Integer>> {
        @Override
        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
            return o2._2.compareTo(o1._2);
        }
    }
}

