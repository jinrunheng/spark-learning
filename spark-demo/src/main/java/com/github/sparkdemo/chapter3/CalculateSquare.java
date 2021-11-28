package com.github.sparkdemo.chapter3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

public class CalculateSquare {

    public static JavaSparkContext connectToSpark() {
        SparkConf conf = new SparkConf()
                .setAppName("CalculateSquare")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public static void calculateSquare() {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        javaRDD.map((Function<Integer, Object>) x -> x * x);
    }

    public static void testFlatMap() {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hello spark"));
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        words.collect().forEach(System.out::println);
    }

    public static void testReduce() {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hello spark"));
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
    }

    public static void testMapToDouble() {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaDoubleRDD result = javaRDD.mapToDouble((DoubleFunction<Integer>) x -> (double) x * x);
        // 求出平均值
        System.out.println(result.mean());
    }

    public static void testPersist() {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> squareRDD = inputRDD.map((Function<Integer, Integer>) x -> x * x);
        squareRDD.persist(StorageLevel.DISK_ONLY());

        System.out.println(squareRDD.count());
        squareRDD.collect().forEach(System.out::println);
    }

    public static void main(String[] args) {
        // testFlatMap();
        // testReduce();
        testMapToDouble();
    }
}
