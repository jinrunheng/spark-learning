package com.github.sparkdemo.chapter9;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class SparkSqlTest {

    public static void sparkSqlTest() {
        JavaSparkContext ctx = new JavaSparkContext();
        SQLContext hiveContext = new HiveContext(ctx);
        String inputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter9/testweet.json";
        SchemaRDD input  = hiveContext.jsonFile(inputFilePath);
    }
}
