package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.types.*;

public class UserDefinedAggregateFunctionDeprecated {
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
        spark.udf().register("averageAge",
               new MyAverageAge());

        spark.sql("select averageAge(age) from user").show();
    }


    //自定义聚合函数类：计算年龄的平均值
    //1.继承UserDefinedAggregateFunction
    //2.重写方法
    static class  MyAverageAge extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction{

        @Override
        public StructType inputSchema() {
            StructField structField = new StructField("age", DataTypes.IntegerType,true, Metadata.empty());

            StructField[] structFieldArray= new StructField[]{structField};
            return new StructType(structFieldArray);
        }

        //缓冲区数据结构
        @Override
        public StructType bufferSchema() {
            StructField total = new StructField("age", DataTypes.IntegerType,true, Metadata.empty());
            StructField count = new StructField("age", DataTypes.IntegerType,true, Metadata.empty());
            StructField[] structFieldArray= new StructField[]{total,count};
            return new StructType((structFieldArray));
        }

        //计算结果的数据类型
        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        //函数稳定性：是否有随机数
        @Override
        public boolean deterministic() {
            return true;
        }

        //缓冲区初始化
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
             buffer.update(0,0);
             buffer.update(1,0);
        }

        //根据输入更新缓冲区
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            buffer.update(0,buffer.getInt(0)+ input.getInt(0));
            buffer.update(1,buffer.getInt(1)+1);
        }

        //缓冲区数据合并
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            buffer1.update(0,buffer1.getInt(0)+buffer2.getInt(0));
            buffer1.update(1,buffer1.getInt(1)+buffer2.getInt(1));
        }

        //计算平均
        @Override
        public Double evaluate(Row buffer) {
            return  (double)buffer.getInt(0)/buffer.getInt(1);
        }
    }

}
