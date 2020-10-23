#!/bin/bash

echo "Waiting for Kafka"
sleep 20s

echo "Starting the Producer application"
sbt run
