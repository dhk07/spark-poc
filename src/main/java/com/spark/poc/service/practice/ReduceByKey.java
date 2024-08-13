package com.spark.poc.service.practice;

import com.spark.poc.service.config.Utils;
import org.apache.spark.api.java.JavaSparkContext;


public class ReduceByKey {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.getSparkConfig();
        RDDExample.pairRddReduceByKey(jsc);
        jsc.close();
    }
}
