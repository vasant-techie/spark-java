package learn.sparkJava;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;

public class SparkApp {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Plan")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().csv("src/main/resources/emp.csv");

        for(int i=0;i<100;i++) {
            df = df.withColumn("dept" + i, lit("D"));
        }
        df.explain(true);
    }
}
