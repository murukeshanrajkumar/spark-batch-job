Aggregator Job
=======================

## Description

A simple Spark Job using Java to perform aggregate on metrics by time stamp. 

## Pre-requisite
1. Java 11
2. Maven 3.X.X
3. [Apache Spark](https://spark.apache.org/downloads.html)

## Build Locally
1. Using Make file
```
make build
```
2. Directly build using maven
```
mvn clean package
```

## Test Locally
1. Using Make file
```
make test
```
2. Directly build using maven
```
mvn clean test
```

## Run Locally

### From IDE like IntelliJ
1. Create a new Run/Debug Configuration
2. Set main class as "org.raj.sample.BatchJob"
3. Set arguments as ```-i "dataset/input" -o "dataset/output" -t "24 hours"```

where argument parameter is as below:
#### -i is the input dataset path, 
#### -o is the output dataset path,
#### -t is the time bucket, default set to 24 hours. 

### From terminal using Spark Submit
```
$(SPARK_SUBMIT_PATH) --class org.raj.sample.BatchJob $(JAR_NAME) -i "dataset/input" -o "dataset/output" -t "24 hours"
```

Replace 
1. SPARK_SUBMIT_PATH to the actual spark submit path configured. Typically available in /opt/homebrew/opt/apache-spark/bin/spark-submit if we use homebrew in mac. Else look up the location by using command ```brew --prefix apache-spark``` 
2. JAR_NAME to the jar built. Typically available as target/spark-batch-job-1.0-SNAPSHOT.jar as per the configuration in pom

where argument parameter is as below:
#### -i is the input dataset path,
#### -o is the output dataset path,
#### -t is the time bucket, default set to 24 hours.

## Debugging possible errors

#### 1. Expected Java Version 11
Check if Java version is correct. Use only Java 11 as any other versions is not compatible for the current repository. 
``` java --version ```
Should return jdk 11. 

#### 2. Build success
Ensure the maven build is success. 

#### 3. Spark installation is right. 
Ensure spark is installed in the right location. The following command makes sure the spark is available. 
```brew --prefix apache-spark```


## Constraints/Systems Limitations
1. Input and Output dataset is of type csv. We can use parquet file format as well for better processing, but using csv for simplicity. 
2. We have a feature to support flexible time buckets for aggregation which is handled by command line options with argument -t. Example ```-t "12 hours```
3. System works under assumption that all columns are always present, hence no added validations is part of system. 
4. The output data is also of type csv that can report average, minimum and maximum values respectively. 
5. All the processing is based on UTC timezone being declared as constant TIMEZONE in BatchJob class. 