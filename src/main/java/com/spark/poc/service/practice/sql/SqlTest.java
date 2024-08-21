package com.spark.poc.service.practice.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlTest {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\racloop\\Documents\\dev-workspace\\hadoop");
        SparkSession session = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir", "C:\\temp")
                .getOrCreate();
        Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/Marks.csv");
        long totalRows = dataset.count();
        Row firstRow = dataset.first();
        String subject = firstRow.getAs("subject").toString();
        firstRow.getAs("year");
        session.close();



    }
}
