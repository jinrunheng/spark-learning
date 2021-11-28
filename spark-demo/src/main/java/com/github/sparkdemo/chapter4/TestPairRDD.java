package com.github.sparkdemo.chapter4;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class TestPairRDD {

    public static JavaSparkContext connectToSpark() {
        SparkConf conf = new SparkConf()
                .setAppName("TestPairRDD")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    public static void testMapToPair() {
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hello spark", "hello wtf"));

        JavaPairRDD<String, String> pairRDD = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], s);
            }
        });
        pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1() + " : " + stringIntegerTuple2._2());
            }
        });
    }

    public static void wordCount() {
        String textPath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter4/hello.txt";
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> lines = sc.textFile(textPath);
        lines
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b)
                .foreach((VoidFunction<Tuple2<String, Integer>>) t -> System.out.println(t._1() + " : " + t._2()));
    }

    public static void wordCount2() {
        String textPath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter4/hello.txt";
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> lines = sc.textFile(textPath);
        Map<String, Long> map = lines
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator())
                .countByValue();
        map.forEach((K, V) -> System.out.println(K + " : " + V));
    }

    public static class AvgCount implements Serializable {
        public int total_;
        public int num_;

        public AvgCount(int total, int num) {
            total_ = total;
            num_ = num;
        }

        public double avg() {
            return total_ / (double) num_;
        }

    }

    public static void calculateKeyAvg() {
        String textPath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter4/test.txt";
        JavaSparkContext sc = connectToSpark();
        JavaRDD<String> lines = sc.textFile(textPath);
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });

        Function<Integer, AvgCount> createAcc = (Function<Integer, AvgCount>) x -> new AvgCount(x, 1);
        Function2<AvgCount, Integer, AvgCount> addAndCount = (Function2<AvgCount, Integer, AvgCount>) (avg, x) -> {
            avg.total_ += x;
            avg.num_ += 1;
            return avg;
        };
        Function2<AvgCount, AvgCount, AvgCount> combine = (Function2<AvgCount, AvgCount, AvgCount>) (avg1, avg2) -> {
            avg1.total_ += avg2.total_;
            avg1.num_ += avg2.num_;
            return avg1;
        };

        JavaPairRDD<String, AvgCount> avgCount = pairRDD.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCount.collectAsMap();

        countMap.forEach((K, V) -> System.out.println(K + " : " + V.avg()));
    }

    public static void main(String[] args) {
        // testMapToPair();
        // calculateSameKeyValueAvg();
        // wordCount2();
        calculateKeyAvg();
    }
}
