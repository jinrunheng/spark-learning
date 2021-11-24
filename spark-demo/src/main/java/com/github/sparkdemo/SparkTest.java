package com.github.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/test.txt";
        String outputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/outputFile.txt";
        JavaRDD<String> input = sc.textFile(filePath).cache();
        // 切分为单词
        JavaRDD<String> words = input.flatMap(
                new FlatMapFunction<String, String>() {

                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) throws Exception {
                return x + y;
            }
        });
        counts.saveAsTextFile(outputFilePath);
    }
}
