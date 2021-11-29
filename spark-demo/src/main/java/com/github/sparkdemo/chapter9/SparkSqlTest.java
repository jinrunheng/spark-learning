package com.github.sparkdemo.chapter9;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;

public class SparkSqlTest {

    public static void sparkSqlTest() {

        // 建立与 Spark 的连接
        SparkConf conf = new SparkConf()
                .setAppName("Spark SQL")
                .setMaster("local");


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .config(conf)
                .getOrCreate();

        String jsonDataSource = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter9/people.json";
        Dataset<Row> df = spark.read().json(jsonDataSource);
        df.show();
        // 编程方式运行 SQL 查询
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT  * FROM people WHERE age >= 19");
        sqlDF.show();

    }

    public static void testGlobalTempView() throws AnalysisException {
        // 建立与 Spark 的连接
        SparkConf conf = new SparkConf()
                .setAppName("Spark SQL")
                .setMaster("local");


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .config(conf)
                .getOrCreate();

        String jsonDataSource = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter9/people.json";
        Dataset<Row> df = spark.read().json(jsonDataSource);
        df.createGlobalTempView("people");
        spark.sql("SELECT * FROM global_temp.people").show();
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
    }

    public static void createRDDOfPersonObjectsFromTextFile(){

        SparkConf conf = new SparkConf()
                .setAppName("Spark SQL")
                .setMaster("local");


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL example")
                .config(conf)
                .getOrCreate();

        String textFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/chapter9/people.txt";
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile(textFilePath)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");
        String sql = "SELECT name FROM people WHERE age BETWEEN 13 AND 19";
        Dataset<Row> teenagersDF = spark.sql(sql);
        Encoder<String> stringEncoder = Encoders.STRING();
        // by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name : " + row.<String>getAs("name");
            }
        }, stringEncoder);

        teenagerNamesByFieldDF.show();
    }



    public static void main(String[] args) throws AnalysisException {

        // sparkSqlTest();
        // testGlobalTempView();
        createRDDOfPersonObjectsFromTextFile();
    }
}
