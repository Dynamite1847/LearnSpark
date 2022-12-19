package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RDDTODataFrame {
    public static void main(String[] args) {
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


        Dataset<Row> personDF = spark.createDataFrame(personJavaRDD, Person.class);
        personDF.show();

        JavaRDD<Row> rowJavaRDD = personDF.toJavaRDD();


    }
}
