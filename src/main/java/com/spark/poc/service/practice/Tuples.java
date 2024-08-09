package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Tuples {

    public static void main(String[] args) {
        findSquareRoot(Utils.getSparkConfig());
    }

    private static void findSquareRoot(JavaSparkContext jsc) {
        List<Integer> numbers = Arrays.asList(4,12,6,8);
        JavaRDD<Integer> javaRDD = jsc.parallelize(numbers);

        JavaRDD<Tuple2<Integer, Double>> squareRoot = javaRDD.map(value -> new Tuple2(value, Math.sqrt(value)));

        squareRoot.foreach(value -> System.out.println(value));
    }
}
