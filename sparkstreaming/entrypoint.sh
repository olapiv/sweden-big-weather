#!/bin/bash

echo "Waiting for Cassandra & Kafka"
sleep 20s

echo "Starting the Spark Streaming application"
sbt run
