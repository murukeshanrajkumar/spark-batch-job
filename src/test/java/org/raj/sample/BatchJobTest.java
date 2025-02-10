package org.raj.sample;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class BatchJobTest {

    private static SparkSession spark;

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder()
                .appName("BatchJobTest")
                .config("spark.sql.session.timeZone", "UTC")
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testAggregateMetrics() {
        List<Row> inputData = Arrays.asList(
                RowFactory.create("temperature", 88.0, "2022-06-04T00:05:00.000Z"),
                RowFactory.create("temperature", 90.0, "2022-06-04T12:30:00.000Z"),
                RowFactory.create("temperature", 92.0, "2022-06-04T23:55:00.000Z"),
                RowFactory.create("precipitation", 0.5, "2022-06-04T01:10:00.000Z"),
                RowFactory.create("precipitation", 0.8, "2022-06-04T14:20:00.000Z"),
                RowFactory.create("precipitation", 0.2, "2022-06-04T22:45:00.000Z")
        );

        StructType schema = new StructType()
                .add("metric", DataTypes.StringType)
                .add("value", DataTypes.DoubleType)
                .add("timestamp", DataTypes.StringType);

        Dataset<Row> inputDataset = spark.createDataFrame(inputData, schema);
        Dataset<Row> result = BatchJob.aggregateMetrics(inputDataset, "24 hours");

        // Check if the expected number of rows is correct (2 metrics in 1 day)
        assertEquals(2, result.count());

        // Convert result to list for easy assertion checking
        List<Row> rows = result.collectAsList();

        for (Row row : rows) {
            String metric = row.getString(0);
            double avgValue = row.getDouble(2);
            double minValue = row.getDouble(3);
            double maxValue = row.getDouble(4);

            if (metric.equals("temperature")) {
                assertEquals((88.0 + 90.0 + 92.0) / 3, avgValue, 0.01);
                assertEquals(88.0, minValue, 0.01);
                assertEquals(92.0, maxValue, 0.01);
            } else if (metric.equals("precipitation")) {
                assertEquals((0.5 + 0.8 + 0.2) / 3, avgValue, 0.01);
                assertEquals(0.2, minValue, 0.01);
                assertEquals(0.8, maxValue, 0.01);
            } else {
                fail("Unexpected metric found: " + metric);
            }
        }

    }

    @Test
    public void testUnsupportedTimeBucket() {
        List<Row> inputData = Arrays.asList(
                RowFactory.create("temperature", 88.0, "2022-06-04T12:01:00.000Z")
        );

        StructType schema = new StructType()
                .add("metric", DataTypes.StringType)
                .add("value", DataTypes.DoubleType)
                .add("timestamp", DataTypes.StringType);

        Dataset<Row> inputDataset = spark.createDataFrame(inputData, schema);

        Exception exception = assertThrows(AnalysisException.class, () -> {
            BatchJob.aggregateMetrics(inputDataset, "abcd").collect();
        });

        assertTrue(exception.getMessage().contains("Unable to parse 'abcd'."));
    }

    @Test
    public void testIsValidTimeBucket() {
        assertTrue(BatchJob.isValidTimeBucket("1 hour"));
        assertTrue(BatchJob.isValidTimeBucket("30 minutes"));
        assertFalse(BatchJob.isValidTimeBucket("5 weeks"));
    }

    @Test
    public void testValidateInputArgumentSuccess() {
        assertDoesNotThrow(() -> BatchJob.validateInputArgument("input", "output", "24 hours"));
    }

    @Test
    public void testValidateInputArgumentNullInputPath() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                BatchJob.validateInputArgument(null, "output", "1 hour"));
        assertEquals("Input and output path must be declared as program argument using -i and -o flag respectively.", exception.getMessage());
    }

    @Test
    public void testValidateOutputArgumentNullInputPath() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                BatchJob.validateInputArgument("output", null, "1 hour"));
        assertEquals("Input and output path must be declared as program argument using -i and -o flag respectively.", exception.getMessage());
    }

    @Test
    public void testValidateInputArgument_InvalidTimeBucket() {
        Exception exception = assertThrows(IllegalArgumentException.class, () ->
                BatchJob.validateInputArgument("input", "output", "5 abcd"));

        assertEquals("Invalid time bucket format. Use formats like '1 hour', '6 hours', '30 minutes', '1 day'.", exception.getMessage());
    }

}
