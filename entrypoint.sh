#!/bin/bash

# For some reason does not work if run as actual Docker entrypoint

echo "Starting the ZooKeeper server"
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

sleep 2

echo "Starting the Kafka server"
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

sleep 5

echo "Creating a Kafka topic"
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic avg

echo "Starting Cassandra"
$CASSANDRA_HOME/bin/cassandra -R

sleep 10

#echo "Starting the cqlsh prompt"
#$CASSANDRA_HOME/bin/cqlsh
