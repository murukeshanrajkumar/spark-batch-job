SHELL := /bin/bash

APP_NAME = spark-batch-job
VERSION = 1.0-SNAPSHOT
JAR_NAME = target/$(APP_NAME)-$(VERSION).jar
INPUT_PATH = dataset/input
OUTPUT_PATH = dataset/output
TIME_BUCKET_DURATION = "24 hours"

build:
	mvn clean package

test:
	mvn clean test

run:
	@echo "Running Spark Job..."
	/usr/local/opt/apache-spark/bin/spark-submit --class org.raj.sample.BatchJob \
	    $(JAR_NAME) -i $(INPUT_PATH) -o $(OUTPUT_PATH) -t $(TIME_BUCKET_DURATION)

clean:
	mvn clean
	@rm -rf $(OUTPUT_PATH)

rm-output:
	@rm -rf $(OUTPUT_PATH)