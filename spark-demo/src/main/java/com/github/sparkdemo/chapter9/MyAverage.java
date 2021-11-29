package com.github.sparkdemo.chapter9;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MyAverage {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Spark SQL")
                .setMaster("local");

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .config(conf)
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .json("/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter9/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();
        Dataset<Row> result = spark.sql(
                "SELECT avg(salary) AS average_salary FROM employees"
        );

        result.show();
        spark.stop();
    }
}
