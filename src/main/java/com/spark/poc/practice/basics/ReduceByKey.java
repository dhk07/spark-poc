package com.spark.poc.practice.basics;

import com.spark.poc.config.Utils;
import org.apache.spark.api.java.JavaSparkContext;


public class ReduceByKey {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
        RDDExample.pairRddReduceByKey(jsc);
        jsc.close();
    }
}
