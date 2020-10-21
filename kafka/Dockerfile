FROM ubuntu:18.04

ENV APP_HOME=/home/dataintensive

################
##### JVM ######
################

RUN apt-get update
RUN yes | apt-get install curl
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install tzdata wget
RUN apt-get update
RUN yes | apt-get install software-properties-common
RUN apt-get update
RUN yes | add-apt-repository ppa:openjdk-r/ppa
RUN apt-get update
RUN yes | apt-get install openjdk-8-jdk-headless
RUN apt-get update

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
WORKDIR $APP_HOME

#########################
##### Kafka #############
#########################

# This includes Zookeeper as well
RUN wget https://apache.uib.no/kafka/2.6.0/kafka_2.12-2.6.0.tgz
RUN tar -xvf kafka_2.12-2.6.0.tgz
ENV KAFKA_HOME=$APP_HOME/kafka_2.12-2.6.0
ENV PATH=$KAFKA_HOME/bin:$PATH

# Entrypoint
ADD ./entrypoint.sh $APP_HOME
RUN chmod +x $APP_HOME/entrypoint.sh
ENTRYPOINT $APP_HOME/entrypoint.sh
