package com.github.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkTest {
    private static void sparkTest() {

        String inputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/hello.txt";
        String outputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/output.txt";

        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFilePath);
        JavaRDD<String> words = input.flatMap((FlatMapFunction<String, String>) s ->  Arrays.asList(s.split(" ")).iterator());
        System.out.println();
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        ).reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);
        counts.saveAsTextFile(outputFilePath);
    }

    private static void testRDDFilter(){
        String logPath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/log.txt";
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile(logPath);
        JavaRDD<String> errorRDD = inputRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        return s.contains("error");
                    }
                }
        );
        System.out.println(errorRDD.count());
    }

    public static void main(String[] args) {
        sparkTest();
        // testRDDFilter();
    }
}
