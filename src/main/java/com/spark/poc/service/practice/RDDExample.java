package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;


public class RDDExample {

    public static void main(String[] args) {
        JavaSparkContext jsc = null;
        try{
            jsc = Utils.getSparkConfig();
//            sumOfList(jsc);
//            logWithPairRdd(jsc);
            pairRddReduceByKey(jsc);
//            calculateSquareRoot(jsc);
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            assert jsc != null;
            jsc.close();
        }
    }

    static void pairRddReduceByKey(JavaSparkContext jsc) {
        List<String> inputData = new ArrayList<>();
        inputData.add( "WARN: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 5 September 0408");
        inputData.add("ERROR: Wednesday 5 September 0409");
        inputData.add("ERROR: Wednesday 12 September 4509");
        inputData.add("WARN: Tuesday 6 September 0430");
        inputData.add("FATAL: Tuesday 7 September 0429");

        JavaRDD<String> javaRDD= jsc.parallelize(inputData);
        JavaPairRDD<String, Integer> pairRDD = javaRDD.mapToPair(rawValue -> {
           String[] values = rawValue.split(":");
           String level = values[0];

           return new Tuple2<>(level, 1);
        });
        JavaPairRDD<String, Integer> logCount = pairRDD.reduceByKey((value1, value2) -> value1+value2);
        logCount.foreach(tuple -> System.out.println(tuple._1+" --->  "+tuple._2));


        // add below command to the vm option if get error - Unable to make field private final byte[]
        //--add-exports java.base/sun.nio.ch=ALL-UNNAMED
    }

    private static void calculateSquareRoot(JavaSparkContext jsc) {

        List<Integer> inputData = Arrays.asList(23,46,12,67,90);
        JavaRDD<Integer> javaRdd = jsc.parallelize(inputData);
        JavaRDD<Double> sqrt = javaRdd.map(value -> Math.sqrt(value));
        sqrt.foreach(value -> System.out.println(value));

        JavaRDD<Long> map = sqrt.map(value -> 1l);
        System.out.println("Map --------------> ");
        map.foreach(value -> System.out.println(value));
        Long count = map.reduce((value1, value2) -> value1+value2);
        System.out.println("Count : "+count);
    }

    private static void sumOfList(JavaSparkContext jsc){
        List<Integer> inputData = Arrays.asList(23,46,12,67,90);
        JavaRDD<Integer> javaRdd = jsc.parallelize(inputData);
        Integer result = javaRdd.reduce((value1, value2) -> value1+value2);
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
