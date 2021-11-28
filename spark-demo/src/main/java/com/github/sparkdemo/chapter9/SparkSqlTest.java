package com.github.sparkdemo.chapter9;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class SparkSqlTest {

    public static void sparkSqlTest() {

        // 建立与 Spark 的连接
        SparkConf conf = new SparkConf()
                .setAppName("Spark SQL")
                .setMaster("local");

        JavaSparkContext ctx = new JavaSparkContext(conf);
        SQLContext hiveContext = new HiveContext(ctx);
        String inputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter9/testweet.json";
        Dataset<Row> rowDataset = hiveContext.jsonFile(inputFilePath);
        // 注册输入的 SchemaRDD
        rowDataset.registerTempTable("tweets");
        // 依据 retweetCount（转发计数）选出推文
        Dataset<Row> topTweets = hiveContext.sql("SELECT text,retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
    }

    public static void main(String[] args) {
        sparkSqlTest();
    }
}
