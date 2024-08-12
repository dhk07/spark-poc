package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapping {

    public static void main(String[] arg){
        JavaSparkContext jsc = Utils.getSparkConfig();
        flatMappingExample(jsc);
        jsc.close();
    }
    private static void flatMappingExample(JavaSparkContext jsc){
        List<String> inputData = new ArrayList<>();
        inputData.add( "WARN: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 5 September 0408");
        inputData.add("ERROR: Wednesday 5 September 0409");
        inputData.add("WARN: Tuesday 6 September 0430");
        inputData.add("FATAL: Tuesday 7 September 0429");

        JavaRDD<String> javaRDD= jsc.parallelize(inputData);
        JavaRDD<String> words = javaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        words.foreach(value -> System.out.println(value));


        // add below command to the vm option if get error
        //--add-exports java.base/sun.nio.ch=ALL-UNNAMED
    }
}
