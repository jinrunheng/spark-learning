package com.github.sparkdemo.chapter3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class FilterWarningAndErrorLogs {

    public static JavaSparkContext connectToSpark() {
        SparkConf conf = new SparkConf()
                .setAppName("FilterLogs")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public static void filter(String logPath) {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> inputRDD = sc.textFile(logPath);
        JavaRDD<String> errorRDD = inputRDD.filter((Function<String, Boolean>) s -> s.contains("error"));
        JavaRDD<String> warningRDD = inputRDD.filter((Function<String, Boolean>) s -> s.contains("warning"));
        JavaRDD<String> badLinesRDD = errorRDD.union(warningRDD);
        // 使用行动操作对错误进行计数
        System.out.println("Input had " + badLinesRDD.count() + " concerning lines");
        System.out.println("Here are 10 examples : ");
        for (String badLine : badLinesRDD.take(10)) {
            System.out.println(badLine);
        }
    }

    public static void main(String[] args) {
        String logPath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter3/log.txt";
        filter(logPath);
    }
}
