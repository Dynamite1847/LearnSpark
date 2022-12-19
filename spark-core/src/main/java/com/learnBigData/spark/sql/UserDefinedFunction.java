package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UserDefinedFunction {
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
        df.createOrReplaceTempView("user");
        spark.udf().register("prefixName",
                (UDF1<String, String>) name -> "Name: "+ name, DataTypes.StringType);

        spark.sql("select age, prefixName(name) from user").show();
    }
}
