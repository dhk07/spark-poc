package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class DataFromFile {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
        getDataFromFile(jsc);
        jsc.close();
    }

    private static void getDataFromFile(JavaSparkContext jsc) {

        JavaRDD<String> dataFromFile = jsc.textFile("src/main/resources/input.txt");
        dataFromFile.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(value -> System.out.println(value));

    }
}
