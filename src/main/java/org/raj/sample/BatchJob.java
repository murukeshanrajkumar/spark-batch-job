package org.raj.sample;

import org.apache.commons.cli.CommandLine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;

public class BatchJob {

    private static final String TIMEZONE = "UTC";

    public static void main(String[] args) {

        // Using CommandLineParser to parse input args
        CommandLine cmd = CommandLineParserUtil.parseArguments(args);
        String inputPath = cmd.getOptionValue("i");
        String outputPath = cmd.getOptionValue("o");
        String timeBucket = cmd.getOptionValue("t", "24 hours");

        // perform input argument validation
        validateInputArgument(inputPath, outputPath, timeBucket);

        // set spark session to use customized time zone
        SparkSession spark = SparkSession.builder()
                .appName("BatchJob")
                .config("spark.sql.session.timeZone", TIMEZONE)
                .master("local[*]")
                .getOrCreate();

        try {
            // read input csv data
            Dataset<Row> data = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(inputPath);

            // process aggregate on the given input data
            Dataset<Row> result = aggregateMetrics(data, timeBucket);

            // write the output report as csv
            result.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath);
        } catch (Exception e) {
            // Can use any other logger. Using system logger for local development
            System.err.println("Error processing the batch: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            // stop spark session by handling it in finally block in case of failures.
            spark.stop();
        }
    }

    public static void validateInputArgument(String inputPath, String outputPath, String timeBucket) {
        // validate input and output path
        if (inputPath == null || outputPath == null) {
            throw new IllegalArgumentException("Input and output path must be declared as " +
                    "program argument using -i and -o flag respectively.");
        }

        // validate time bucket input argument
        if (!isValidTimeBucket(timeBucket)) {
            String message = "Invalid time bucket format. Use formats like '1 hour', '6 hours', '30 minutes', '1 day'.";
            // Can use any other logger. Using system logger for local development
            System.err.println(message);
            throw new IllegalArgumentException(message);
        }
    }

    public static Dataset<Row> aggregateMetrics(Dataset<Row> data, String timeBucket) {
        return data
                .withColumn("timestamp", to_utc_timestamp(col("timestamp"), TIMEZONE))
                .withColumn("time_bucket", window(col("timestamp"), timeBucket))
                .withColumn("time_bucket_start", to_utc_timestamp(col("time_bucket").getField("start"), TIMEZONE))
                .groupBy(col("metric"), col("time_bucket_start"))
                .agg(
                        avg("value").alias("avg_value"),
                        min("value").alias("min_value"),
                        max("value").alias("max_value")
                )
                .orderBy("time_bucket_start");
    }

    public static boolean isValidTimeBucket(String timeBucket) {
        String regex = "^(\\d+ (second|minute|hour|day)(s)?)$";
        return Pattern.matches(regex, timeBucket);
    }

}