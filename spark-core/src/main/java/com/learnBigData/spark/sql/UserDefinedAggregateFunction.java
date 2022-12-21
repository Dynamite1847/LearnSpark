package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

public class UserDefinedAggregateFunction {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark sql intro");
        SparkSession spark = SparkSession
                .builder().enableHiveSupport()
                .config(sparkConf)
                .getOrCreate();

        //DataFrame
        Dataset<Row> df = spark.read().json("/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user.json").cache();
        df.createOrReplaceTempView("user");
        spark.udf().register("averageAge", functions.udaf(new MyAverageAge(),Encoders.INT()));

        spark.sql("select averageAge(age) from user").show();
    }


    //自定义聚合函数类：计算年龄的平均值
    //1.继承UserDefinedAggregateFunction
    //2.重写方法

    static class MyAverageAge extends Aggregator<Integer,Average,Double>{


        @Override
        public Average zero() {
            return new Average(0,0);
        }

        @Override
        public Average reduce(Average buffer, Integer age) {
            buffer.setCount(buffer.getCount()+1);
            buffer.setTotal(buffer.getTotal()+age);
            return buffer;
        }

        @Override
        public Average merge(Average buffer1, Average buffer2) {
            buffer1.setCount(buffer1.getCount()+buffer2.getCount());
            buffer1.setTotal(buffer1.getTotal()+buffer2.getTotal());
            return buffer1;
        }

        @Override
        public Double finish(Average reduction) {
            return (double)reduction.getTotal()/ reduction.getCount();
        }

        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE() ;
        }
    }

    public static class Average implements Serializable {
        Integer count ;
        Integer total;

        public Average(Integer count, Integer total) {
            this.count = count;
            this.total = total;
        }

        public Average() {
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }
    }
}
