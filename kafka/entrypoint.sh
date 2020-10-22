#!/bin/bash

echo "Creating a Kafka topic city-temperatures"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic city-temperatures

echo "Creating a Kafka topic grid-temperatures"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic grid-temperatures
