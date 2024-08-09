package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class RDDExample {

    public static void main(String[] args) {
        JavaSparkContext jsc = null;
        try{
            jsc = Utils.getSparkConfig();
//            sumOfList(jsc);
            logWithPairRdd(jsc);

        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            assert jsc != null;
            jsc.close();
        }



    }

    private static void sumOfList(JavaSparkContext jsc){
        List<Integer> inputData = Arrays.asList(23,46,12,67,90);
        JavaRDD<Integer> javaRdd = jsc.parallelize(inputData);
        Integer result = javaRdd.reduce(Integer::sum);
        System.out.println("Result: "+result);
    }

    private static void logWithPairRdd(JavaSparkContext jsc){
        List<String> inputData = new ArrayList<>();
        inputData.add( "WARN: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 5 September 0408");
        inputData.add("ERROR: Wednesday 5 September 0409");
        inputData.add("WARN: Tuesday 6 September 0430");
        inputData.add("FATAL: Tuesday 7 September 0429");

        JavaRDD<String> javaRDD= jsc.parallelize(inputData);
        JavaRDD<String> words = javaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        JavaRDD<String> filter = words.filter(word -> word.length()>1);
//       words.foreach(value -> System.out.println(value));     //will print all data
        filter.foreach(value -> System.out.println(value));
    }

}
