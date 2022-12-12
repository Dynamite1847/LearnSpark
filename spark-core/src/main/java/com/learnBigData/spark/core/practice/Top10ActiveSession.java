package com.learnBigData.spark.core.practice;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

public class Top10ActiveSession implements Serializable {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Top10HotCategory");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //1.读取原始日志数据
        JavaRDD<String> userActionRDD = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user_visit_action.txt");
        userActionRDD.cache();
        //2.转换数据结构
        List<String> top10Click = Arrays.asList(getTop10Click(userActionRDD));

        for(String click :top10Click){
            System.out.println(click);
        }
        System.out.println("========================");
        //3.过滤原始数据，保留点击和前十品类ID
        JavaRDD<String> filterActionRDD = userActionRDD.filter(x -> {
            String[] datas = x.split("_");
            if (!datas[6].equals("-1")) {
                return top10Click.contains(datas[6]);
            }else return false;
        });
        //4.根据品类ID和sessionID进行点击量统计
        JavaPairRDD<Tuple2<String, String>, Integer> reducePairRDD = filterActionRDD.mapToPair(action -> {
            String[] datas = action.split("_");
            return new Tuple2<>(new Tuple2<>(datas[6], datas[2]), 1);
        }).reduceByKey((Integer x, Integer y) -> x + y);
        //5.将统计结果进行结构转换
        JavaPairRDD<String, Tuple2<String, Integer>> reduceMapRDD = reducePairRDD.mapToPair((x -> new Tuple2<>(x._1()._1, new Tuple2(x._1._2(), x._2))));

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> reduceGroupRDD = reduceMapRDD.groupByKey();


        JavaPairRDD<String, Object> resultRDD = reduceGroupRDD.mapValues((Function<Iterable<Tuple2<String, Integer>>, Object>) x -> {
            List list = IteratorUtils.toList(x.iterator());
            list.sort(new myCompare());
            return list.subList(0, 9);
        });
        resultRDD.foreach(x -> System.out.println(x));

        //6.将结果打印到控制台
        sparkContext.stop();
    }

    private static String[] getTop10Click(JavaRDD<String> userActionRDD) {
        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> resultRDD = userActionRDD.flatMap(Top10ActiveSession::call);

        //5.根据数量进行排序，并取前十名
        //点击数量排序，下单数量排序，支付数量排序
        //元组排序，先比较第一个，再比较第二个，再比较第三个，以此类推
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> unionPairRDD = resultRDD.mapToPair(x -> x);
        //（品类id，（点击数量，下单数量，支付数量））
        JavaPairRDD<String, Tuple3<Integer, Integer, Integer>> resultPairRDD = unionPairRDD.reduceByKey((t1, t2) -> new Tuple3<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3()));

        JavaRDD<Tuple2<String, Tuple3<Integer, Integer, Integer>>> analysisRDD = resultPairRDD.map(x -> x);

        List<Tuple2<String, Tuple3<Integer, Integer, Integer>>> resultsList = analysisRDD.sortBy(x -> x._2()._1(), false, 2).take(10);
        return resultsList.stream().map(x -> x._1).toArray(value -> new String[value]);
    }

    static Iterator<Tuple2<String, Tuple3<Integer, Integer, Integer>>> call(String action) {
        String[] splitData = action.split("_");
        if (Integer.parseInt(splitData[6]) != -1) {
            String[] splitItems = new String[1];
            splitItems[0] = splitData[6];
            return Arrays.stream(splitItems).map(id -> new Tuple2<>(id, new Tuple3<>(1, 0, 0))).iterator();
        } else if (!splitData[8].equals("null")) {
            String[] splitItems = splitData[8].split(",");
            return Arrays.stream(splitItems).map(id -> new Tuple2<>(id, new Tuple3<>(0, 1, 0))).iterator();
        } else if (!splitData[10].equals("null")) {
            String[] splitItems = splitData[10].split(",");
            return Arrays.stream(splitItems).map(id -> new Tuple2<>(id, new Tuple3<>(0, 0, 1))).iterator();
        } else return new Iterator<Tuple2<String, Tuple3<Integer, Integer, Integer>>>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Tuple2<String, Tuple3<Integer, Integer, Integer>> next() {
                return null;
            }
        };
    }

    static class myCompare implements Comparator<Tuple2<String, Integer>> {

        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t2._2() - t1._2();
        }
    }
}


