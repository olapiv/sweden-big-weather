#!/bin/bash

echo "Waiting for Spark Streaming application"
sleep 60s

echo "Starting the SparkSQL/Calculations application"
sbt run