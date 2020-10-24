#!/bin/bash

echo "Waiting for Cassandra & Kafka"
sleep 30s

echo "Starting the Spark Streaming application"
sbt run
