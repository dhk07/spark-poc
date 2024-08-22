package com.spark.poc.practice.basics;

import com.spark.poc.config.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class DataFromFile {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
        getDataFromFile(jsc);
        replaceMethod(jsc);
        jsc.close();
    }

    private static void getDataFromFile(JavaSparkContext jsc) {
        JavaRDD<String> dataFromFile = jsc.textFile("src/main/resources/input.txt");
        dataFromFile.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .foreach(value -> System.out.println(value));
    }

    private static void replaceMethod(JavaSparkContext jsc) {

        JavaRDD<String> dataFromFile = jsc.textFile("src/main/resources/input.txt");
        JavaRDD<String> onlyAlphabate = dataFromFile.map(value -> value.replaceAll( "[^a-zA-Z ]", "").toLowerCase());

        JavaRDD<String> removeBlankLine = onlyAlphabate.filter(value -> value.length()>0);
        List<String> result = removeBlankLine.take(50);
        result.forEach(System.out::println);

        // add below command to the vm option if get error
        //--add-exports java.base/sun.nio.ch=ALL-UNNAMED

    }
}
