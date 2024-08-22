package com.spark.poc.practice.sql;

import com.spark.poc.config.Utils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SqlFilter {

    public static void main(String[] args) {
        SparkSession sparkSession = Utils.getSparkSession();
        filterExample2(sparkSession);
        sparkSession.close();
    }

    private static void filterExample(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/Marks.csv");
        Dataset<Row> filteredResult = dataset.filter("subject = 'Math' AND year <2019");
        filteredResult.show();
    }
    private static void filterExample2(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/Marks.csv");
        Column subject = dataset.col("subject");
        Column year = dataset.col("year");
        Dataset<Row> filteredResult = dataset.filter(subject.equalTo("Math").and(year.gt(2019)));
        filteredResult.show();
    }

    private static void filterExample3(SparkSession sparkSession) {
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/Marks.csv");
        Dataset<Row> filteredResult = dataset.filter(
                col("subject").equalTo("Math")
                        .and(col("year").gt(2019)));
        filteredResult.show();
    }
}
