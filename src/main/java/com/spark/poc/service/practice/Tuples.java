package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Tuples {

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
//        findSquareRoot(jsc);
        sortedValue(jsc);
    }

    private static void sortedValue(JavaSparkContext jsc) {

        JavaRDD<String> fileData = jsc.textFile("src/main/resources/input.txt");
        JavaRDD<String> onlyLetters = fileData.map(value -> value.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        JavaRDD<String> removeBlankLine = onlyLetters.filter(value-> !value.trim().isEmpty());
        JavaRDD<String> flatWords = removeBlankLine.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> removedBlankWords = flatWords.filter(value -> !value.isEmpty());
        JavaPairRDD<String, Integer> pairRDD = removedBlankWords.mapToPair(value -> new Tuple2<>(value, 1));
        JavaPairRDD<String, Integer> total = pairRDD.reduceByKey(Integer::sum);
        JavaPairRDD<Integer, String > switchedData = total.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
//        JavaPairRDD<Integer, String> sortedData = switchedData.sortByKey();
        JavaPairRDD<Integer, String> sortedData = switchedData.sortByKey(false);

        sortedData = sortedData.coalesce(1);
        System.out.println("NO of partitions: "+sortedData.getNumPartitions());
        List<Tuple2<Integer, String>> result = sortedData.take(100);
        result.forEach(System.out::println);
    }

    private static void findSquareRoot(JavaSparkContext jsc) {
        List<Integer> numbers = Arrays.asList(4,12,6,8);
        JavaRDD<Integer> javaRDD = jsc.parallelize(numbers);

        JavaRDD<Tuple2<Integer, Double>> squareRoot = javaRDD.map(value -> new Tuple2(value, Math.sqrt(value)));

        squareRoot.foreach(value -> System.out.println(value));

        // add below command to the vm option if get error
        //--add-exports java.base/sun.nio.ch=ALL-UNNAMED
    }
}
