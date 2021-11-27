package com.github.sparkdemo.lesson2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args) {

        // 建立与 Spark 的连接
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. 读取文件，获取一行一行的数据
        String filePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/hello.txt";
        JavaRDD<String> lines = sc.textFile(filePath);

        // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
        // hello word / hello spark => hello,word,hello,spark
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 3. 将数据根据单词进行分组，便于统计（转换为 <word,1> 的格式）
        // (hello,hello),(world),(spark)
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 4. 对分组后的数据进行聚合，统计相同 word 出现的频率
        // (hello,2),(world,1),(spark,1)
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 5. 执行 action 将结果打印出来
        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1() + " " + t._2());
            }
        });

        // 6. 将结果输出到一个目录中
        String outputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/output";
        wordCount.saveAsTextFile(outputFilePath);

        // 关闭 SparkContext
        sc.stop();
    }
}
