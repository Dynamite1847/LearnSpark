package com.learnBigData.spark.core.practice;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

import java.util.*;


public class PageFlowAnalysis {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Top10HotCategory");
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> sourceRDD = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user_visit_action.txt");
            JavaRDD<UserVisitAction> actionRDD = sourceRDD.map(action -> {
                String[] sourceData = action.split("_");
                return new UserVisitAction(sourceData[0], sourceData[1], sourceData[2], Long.valueOf(sourceData[3]), sourceData[4], sourceData[5], Long.valueOf(sourceData[6]), Long.valueOf(sourceData[7]), sourceData[8], sourceData[9], sourceData[10], sourceData[11], Long.valueOf(sourceData[12]));
            });
            actionRDD.cache();

            List<Long> wantedIDs = Arrays.asList(1L,2L,3L,4L,5L,6L,7L);
            List<Tuple3<Long,Long,Integer>> wantedZipList = new ArrayList<>();
            for (int i = 0; i < wantedIDs.size() - 1; i++) {
                wantedZipList.add(new Tuple3<>(wantedIDs.get(i),wantedIDs.get(i+1),1));
            }
            for (Tuple3<Long,Long,Integer> wanted: wantedZipList){
                System.out.println(wanted);
            }

            JavaRDD<UserVisitAction> filteredActionRDD = actionRDD.filter(x -> wantedIDs.contains(x.getPageId()));

            JavaPairRDD<Long, Integer> pageCountRDD = filteredActionRDD.mapToPair(userVisitAction -> new Tuple2<>(userVisitAction.getPageId(), 1)).reduceByKey(Integer::sum);
            filteredActionRDD.cache();
            Map<Long, Integer> pageCountMap = pageCountRDD.collectAsMap();
            JavaPairRDD<String, Iterable<UserVisitAction>> sessionRDD = filteredActionRDD.groupBy(UserVisitAction::getSessionId);

            // 【1，2，3，4】
            // 【1，2】，【2，3】，【3，4】
            // 【1-2，2-3，3-4】
            // Sliding : 滑窗
            // 【1，2，3，4】
            // 【2，3，4】
            // zip : 拉链
            JavaPairRDD<String, List<Tuple3<Long,Long,Integer>>> clickFlowPairRDD = sessionRDD.mapValues(iter -> {
                List<UserVisitAction> list = IteratorUtils.toList(iter.iterator());
                list.sort(new MyComparator());
                Long[] sortedPageID = list.stream().map(UserVisitAction::getPageId).toArray(Long[]::new);
                List<Tuple3<Long,Long,Integer>> zippedList = new ArrayList<>();
                for (int i = 0; i < sortedPageID.length - 1; i++) {
                    zippedList.add(new Tuple3<>(sortedPageID[i], sortedPageID[i + 1],1));
                }
                return zippedList;
            });
            JavaRDD<Tuple3<Long, Long, Integer>> clickFlowRDD = clickFlowPairRDD.flatMap(x -> x._2.iterator());
            JavaRDD<Tuple3<Long, Long, Integer>> filteredClickFlowRDD = clickFlowRDD.filter(wantedZipList::contains);
            filteredClickFlowRDD.foreach(x-> System.out.println(x));
            JavaPairRDD<Tuple2<Long, Long>, Integer> clickResultPairRDD = filteredClickFlowRDD.mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1(), x._2()), x._3()));
            JavaPairRDD<Tuple2<Long, Long>, Integer> clickResultSum = clickResultPairRDD.reduceByKey(Integer::sum);

            //计算转化率
            clickResultSum.foreach(x->{
                Integer clickResult = pageCountMap.getOrDefault(x._1._1,0);
                System.out.println("From page " +x._1._1+" to " + x._1._2 + " , the click to rate is " + Double.valueOf(x._2)/clickResult);
            });

        }
    }

    static class MyComparator implements Comparator<UserVisitAction> {
        @Override
        public int compare(UserVisitAction u1, UserVisitAction u2) {
            return u1.getActionTime().compareTo(u2.getActionTime());
        }
    }
}

