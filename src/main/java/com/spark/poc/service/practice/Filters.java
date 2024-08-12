package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Filters {

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
        filterExample(jsc);
    }
    private static void filterExample(JavaSparkContext jsc){
        List<String> inputData = new ArrayList<>();
        inputData.add( "WARN: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 5 September 0408");
        inputData.add("ERROR: Wednesday 5 September 0409");
        inputData.add("WARN: Tuesday 6 September 0430");
        inputData.add("FATAL: Tuesday 7 September 0429");

        jsc.parallelize(inputData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.length()>1)
                .foreach(value -> System.out.println(value));
    }
}
