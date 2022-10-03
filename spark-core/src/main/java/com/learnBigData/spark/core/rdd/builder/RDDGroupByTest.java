package com.learnBigData.spark.core.rdd.builder;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import javax.swing.plaf.IconUIResource;
import java.text.SimpleDateFormat;

import java.util.*;


public class RDDGroupByTest {
    public static void main(String[] args) {
        //准备环境
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Operator");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sparkContext.textFile("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/apache.log");
        JavaPairRDD<Integer, Iterable<String>> timeRDD = rdd.groupBy(line -> {
            String[] data = line.split(" ");
            String time = data[3];
            SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Date date = sdf.parse(time);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            return hour;
        });


        for (Tuple2<Integer, Iterable<String>> hour : timeRDD.collect()) {
            Iterable<String> it = hour._2;
            Iterator<String> iterator = it.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                count++;
                iterator.next();
            }
            System.out.println(hour._1+"h :"+count);
        }
    }
}
