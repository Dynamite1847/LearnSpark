package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

public class RDDToDataset {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark sql intro");
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        //RDD

        JavaRDD<Person> personJavaRDD = spark.read().json("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user.json").javaRDD()
                .map(x -> {
                    Person person = new Person();
                    person.setName((String) x.get(1));
                    person.setAge(Integer.parseInt(String.valueOf(x.getLong(0))));
                    return person;
                });

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> personDataset = spark.createDataFrame(personJavaRDD, Person.class).as(personEncoder);

        JavaRDD<Person> personJavaRDD1 = personDataset.rdd().toJavaRDD();
    }
}
