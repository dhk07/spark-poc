package com.spark.poc.practice.basics;

import com.spark.poc.config.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ObjectRDD {

    public static void main(String[] args) {
        getSquare(Utils.getSparkConfig());
    }

    private static void getSquare(JavaSparkContext jsc){
        List<Integer> numbers = Arrays.asList(4,12,6,8);
        JavaRDD<Integer> javaRDD = jsc.parallelize(numbers);
        JavaRDD<SquareNumber> square = javaRDD.map(value -> new SquareNumber(value));
        square.foreach(value -> System.out.println(value.toString()));

        // add below command to the vm option if get error -
        //--add-exports java.base/sun.nio.ch=ALL-UNNAMED
    }
}
