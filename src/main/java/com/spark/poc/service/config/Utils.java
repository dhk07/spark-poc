package com.spark.poc.service.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.logging.Level;
import java.util.logging.Logger;
public class Utils {


    public static JavaSparkContext getSparkConfig(){
//        System.setProperty("hadoop.home.dir", "C:\\Users\\dhiru\\Documents\\hadoop");
        SparkConf conf = new SparkConf().setAppName("Starting spark").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        return jsc;
    }
}
