package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class CreateDataSet {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark sql intro");
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        //DataSet
        //Dataset是特定泛型的DataFrame
        Dataset<Integer> ds = spark.createDataset(Arrays.asList(1, 3, 4, 5, 2, 6), Encoders.INT());

        ds.show();
    }
}
