package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class PracticeImport {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark sql intro");
        SparkSession spark = SparkSession
                .builder()
                .enableHiveSupport()
                .config(sparkConf)
                .getOrCreate();

//        spark.sql("CREATE TABLE if `user_visit_action`(\n" +
//                "  `date` string,\n" +
//                "  `user_id` bigint,\n" +
//                "  `session_id` string,\n" +
//                "`page_id` bigint, `action_time` string, `search_keyword` string, `click_category_id` bigint, `click_product_id` bigint, `order_category_ids` string, `order_product_ids` string, `pay_category_ids` string, `pay_product_ids` string, `city_id` bigint)\n" +
//                "row format delimited fields terminated by '\\t';");
//        spark.sql("load data local inpath '/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/user_visit_action1.txt' into table\n" +
//                "user_visit_action");

//        spark.sql("CREATE TABLE `product_info`(\n" +
//                "  `product_id` bigint,\n" +
//                "  `product_name` string,\n" +
//                "  `extend_info` string)\n" +
//                "row format delimited fields terminated by '\\t';\n" );
//                spark.sql("load data local inpath '/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/product_info.txt' into table product_info;");
//
//        spark.sql("CREATE TABLE `city_info`(\n" +
//                "  `city_id` bigint,\n" +
//                "  `city_name` string,\n" +
//                "  `area` string)\n" +
//                "row format delimited fields terminated by '\\t';\n" );
//              spark.sql(  "load data local inpath '/Users/dongyu/IdeaProjects/LearnSpark/spark-core/src/main/resources/city_info.txt' into table city_info;");

        spark.sql("select* from (select *, rank() over(partition by area order by click_count desc) as rank from(select area, product_name, count(*) as click_count from (select u.*, p.product_name, c.area, c.city_name from user_visit_action u left join product_info p on u.click_product_id = p.product_id left join city_info c on u.city_id =c.city_id where u.click_product_id >-1) t1 group by area,product_name ) t2 ) t3 where rank <=3").show();
    }
}
