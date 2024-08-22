package com.spark.poc.practice.sql;

import com.spark.poc.config.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvReader {

    public static void main(String[] args) {
        SparkSession sparkSession = Utils.getSparkSession();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/Marks.csv");
        dataset.show();
        long totalRows = dataset.count();
        System.out.println("Total row: "+totalRows);
        Row firstRow = dataset.first();
        String subject = firstRow.getAs("subject").toString();
        System.out.println("Subject : "+subject);
        int years= Integer.parseInt(firstRow.getAs("year"));
        System.out.println(years);
        sparkSession.close();



    }
}
