package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;


public class CreateDataFrame {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark sql intro");
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        //DataFrame
        Dataset<Row> df = spark.read().json("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user.json").cache();
        df.show();
        df.createOrReplaceTempView("people");
        spark.sql("select * from people").show();

        df.select(df.col("name"), df.col("age").plus(1)).show();


        //RDD<=>DataFrame


        //RDD<=>DataSet



        spark.close();
    }
}
