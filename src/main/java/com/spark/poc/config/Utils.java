package com.spark.poc.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Level;
import java.util.logging.Logger;
public class Utils {


    public static JavaSparkContext getSparkConfig(){
        System.setProperty("hadoop.home.dir", "C:\\Users\\dhiru\\Documents\\hadoop");

        SparkConf conf = new SparkConf().setAppName("Starting spark").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        return jsc;
    }

    public static SparkSession getSparkSession(){
        System.setProperty("hadoop.home.dir", "C:\\Users\\racloop\\Documents\\dev-workspace\\hadoop");
        SparkSession session = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "C:\\temp")
                .getOrCreate();
        return session;
    }
}
