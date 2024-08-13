package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class JoinExample {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
//        innerJoin(jsc);
//        leftOuterJoin(jsc);
//        rightOuterJoin(jsc);
        cartesian(jsc);
        jsc.close();

    }

    private static void leftOuterJoin(JavaSparkContext jsc) {
        List<Tuple2<Integer, Integer>> visitData = Arrays.asList(
                new Tuple2<>(1, 9)
                , new Tuple2<>(3, 16)
                , new Tuple2<>(10, 23));
        List<Tuple2<Integer, String >> userData = Arrays.asList(
                new Tuple2<>(1, "Ramesh")
                , new Tuple2<>(2, "Mukesh")
                , new Tuple2<>(3, "Raju")
                , new Tuple2<>(4, "Rohan")
                , new Tuple2<>(5, "Karan")
                , new Tuple2<>(6, "Rajesh")
        );

        JavaPairRDD<Integer, Integer> visitRdd = jsc.parallelizePairs(visitData);
        JavaPairRDD<Integer, String> userRdd = jsc.parallelizePairs(userData);

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> userVisitData =  visitRdd.leftOuterJoin(userRdd);

        userVisitData.foreach(value -> System.out.println(value._2._2.orElse("blank").toUpperCase()));
    }

    private static void innerJoin(JavaSparkContext jsc) {

        List<Tuple2<Integer, Integer>> visitData = Arrays.asList(
                new Tuple2<>(1, 10)
                , new Tuple2<>(3, 16)
                , new Tuple2<>(10, 23));
        List<Tuple2<Integer, String >> userData = Arrays.asList(
                new Tuple2<>(1, "Ramesh")
                , new Tuple2<>(2, "Mukesh")
                , new Tuple2<>(3, "Raju")
                , new Tuple2<>(4, "Rohan")
                , new Tuple2<>(5, "Karan")
                , new Tuple2<>(6, "Rajesh")
        );

        JavaPairRDD<Integer, Integer> visitRdd = jsc.parallelizePairs(visitData);
        JavaPairRDD<Integer, String> userRdd = jsc.parallelizePairs(userData);

        JavaPairRDD<Integer, Tuple2<Integer, String>> userVisitData =  visitRdd.join(userRdd);
        userVisitData.foreach(value -> System.out.println(value));




    }
    private static void rightOuterJoin(JavaSparkContext jsc) {
        List<Tuple2<Integer, Integer>> visitData = Arrays.asList(
                new Tuple2<>(1, 9)
                , new Tuple2<>(3, 16)
                , new Tuple2<>(10, 23));
        List<Tuple2<Integer, String >> userData = Arrays.asList(
                new Tuple2<>(1, "Ramesh")
                , new Tuple2<>(2, "Mukesh")
                , new Tuple2<>(3, "Raju")
                , new Tuple2<>(4, "Rohan")
                , new Tuple2<>(5, "Karan")
                , new Tuple2<>(6, "Rajesh")
        );

        JavaPairRDD<Integer, Integer> visitRdd = jsc.parallelizePairs(visitData);
        JavaPairRDD<Integer, String> userRdd = jsc.parallelizePairs(userData);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> userVisitData =  visitRdd.rightOuterJoin(userRdd);

        userVisitData.foreach(value -> System.out.println(value._2._2 +" has visited "+value._2._1.orElse(0)+" times"));
    }

    private static void cartesian(JavaSparkContext jsc) {
        List<Tuple2<Integer, Integer>> visitData = Arrays.asList(
                new Tuple2<>(1, 9)
                , new Tuple2<>(3, 16)
                , new Tuple2<>(10, 23));
        List<Tuple2<Integer, String >> userData = Arrays.asList(
                new Tuple2<>(1, "Ramesh")
                , new Tuple2<>(2, "Mukesh")
                , new Tuple2<>(3, "Raju")
                , new Tuple2<>(4, "Rohan")
                , new Tuple2<>(5, "Karan")
                , new Tuple2<>(6, "Rajesh")
        );

        JavaPairRDD<Integer, Integer> visitRdd = jsc.parallelizePairs(visitData);
        JavaPairRDD<Integer, String> userRdd = jsc.parallelizePairs(userData);

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> userVisitData =  visitRdd.cartesian(userRdd);

        userVisitData.foreach(value -> System.out.println(value));

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

}
