package com.learnBigData.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Practice {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Spark sql intro");
        SparkSession spark = SparkSession
                .builder()
                .enableHiveSupport()
                .config(sparkConf)
                .getOrCreate();

        //查询基本数据
        spark.sql("select u.*, p.product_name, c.area, c.city_name from user_visit_action u left join product_info p on u.click_product_id = p.product_id left join city_info c on u.city_id =c.city_id where u.click_product_id >-1").createOrReplaceTempView("t1");
        spark.sql("select * from t1").show();
        //联合城市信息
        spark.udf().register("cityStat", functions.udaf(new CityStatUDAF(), Encoders.STRING()));

        spark.sql("select area, product_name, count(*) as click_count, cityStat(city_name) as city_stat from  t1 group by area,product_name").createOrReplaceTempView("t2");
        spark.sql("select * from t2").show();
        //区域内点击数量排名
        spark.sql("select *, rank() over(partition by area order by click_count desc) as rank from t2").createOrReplaceTempView("t3");


        //取前三名
        spark.sql("select * from t3 where rank <=3").show(false);
    }

    public static class CityStatBuf{
        Long total;
        HashMap<String, Long> cityMap;

        public CityStatBuf() {
        }

        public CityStatBuf(Long total, HashMap<String, Long> cityMap) {
            this.total = total;
            this.cityMap = cityMap;
        }

        public Long getTotal() {
            return total;
        }

        public void setTotal(Long total) {
            this.total = total;
        }

        public HashMap<String, Long> getCityMap() {
            return cityMap;
        }


    }

     static class CityStatUDAF extends Aggregator<String,CityStatBuf, String>{

        @Override
        public CityStatBuf zero() {
            return new CityStatBuf(0L, new HashMap<>());
        }

        @Override
        public CityStatBuf reduce(CityStatBuf b, String city) {
            b.setTotal(b.getTotal()+1);
            Long count = b.getCityMap().getOrDefault(city,0L)+1;
            HashMap<String, Long> cityMap = b.getCityMap();
            cityMap.put(city,count);
            return b;
        }

        @Override
        public CityStatBuf merge(CityStatBuf b1, CityStatBuf b2) {
            b1.setTotal(b1.getTotal()+b2.getTotal());
            HashMap<String, Long> cityMap1 = b1.getCityMap();
            HashMap<String, Long> cityMap2 = b2.getCityMap();
            if (cityMap2!=null) {
                cityMap1.forEach((key, value) -> {
                    cityMap1.put(key, cityMap2.getOrDefault(key, 0L) + value);
                });
            }
            return b1;
        }

        @Override
        public String finish(CityStatBuf cityStatBuf) {
            Long total = cityStatBuf.getTotal();
            HashMap<String, Long> cityMap = cityStatBuf.getCityMap();
            LinkedHashMap<String,Long> sorted = cityMap.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            String result = new String();
            int count = 0;
            Long sum = 0L;
            for (Map.Entry<String, Long> entry : sorted.entrySet()) {
                String key = entry.getKey();
                Long value = entry.getValue();
                if (count<2){
                result = String.format("%s%s for a clicking rate %d%%, ", result, key, value / total * 100);
                sum+= value;
                count++;
                }
            }
            result= result+ "Others for a clicking rate " + (total-sum)/total;

            return result;
        }

        @Override
        public Encoder<CityStatBuf> bufferEncoder() {
            return Encoders.bean(CityStatBuf.class);
        }

        @Override
        public Encoder<String> outputEncoder() {
            return Encoders.STRING();
        }
    }

}
