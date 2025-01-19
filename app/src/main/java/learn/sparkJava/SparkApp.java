package learn.sparkJava;

import org.apache.spark.sql.SparkSession;

public class SparkApp {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Plan")
                .master("local[*]")
                .getOrCreate();
        spark.read().csv("src/main/resources/emp.csv").show();
    }
}
