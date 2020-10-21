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
##### ZooKeeper #############
#########################

RUN wget https://apache.uib.no/zookeeper/zookeeper-3.6.2/apache-zookeeper-3.6.2-bin.tar.gz
RUN tar  -xvf apache-zookeeper-3.6.2-bin.tar.gz
ENV ZOOKEEPER_HOME=$APP_HOME/apache-zookeeper-3.6.2-bin
ENV PATH=$ZOOKEEPER_HOME/bin:$PATH

#########################
##### Kafka #############
#########################

RUN wget https://apache.uib.no/kafka/2.6.0/kafka_2.12-2.6.0.tgz
RUN tar -xvf kafka_2.12-2.6.0.tgz
ENV KAFKA_HOME=$APP_HOME/kafka_2.12-2.6.0
ENV PATH=$KAFKA_HOME/bin:$PATH

#########################
##### Python 2.7 ########
#########################

RUN yes | apt install python-minimal
#RUN yes | apt-get install python2
ENV PYTHONPATH=/usr/bin/python2

####################################
##### Cassandra & Anaconda #########
####################################
RUN wget https://archive.apache.org/dist/cassandra/3.11.2/apache-cassandra-3.11.2-bin.tar.gz
RUN tar -xvf apache-cassandra-3.11.2-bin.tar.gz
RUN wget https://repo.anaconda.com/archive/Anaconda2-5.2.0-Linux-x86_64.sh
ENV CASSANDRA_HOME=$APP_HOME/apache-cassandra-3.11.2
ENV PATH=$CASSANDRA_HOME/bin:$PATH
ENV PATH=$PYTHONPATH/bin:$CASSANDRA_HOME/bin:$PATH

#########################
##### Apache Spark ######
#########################
RUN wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz 
RUN tar -xvf spark-2.4.3-bin-hadoop2.7.tgz
ENV SPARK_HOME=$APP_HOME/spark-2.4.3-bin-hadoop2.7
ENV PATH=$SPARK_HOME/bin:$PATH

################
##### SBT ######
################
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 
RUN apt-get update
RUN yes | apt-get install sbt

# Generate ssh key without password
RUN mkdir -p $HOME/.ssh
RUN ssh-keygen -t rsa -P "" -f $HOME/.ssh/id_rsa

# Copy id_rsa.pub to authorized-keys
RUN mkdir -p $HOME/.ssh
RUN cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

ADD ./src $APP_HOME/src
ADD ./entrypoint.sh $APP_HOME

#RUN chmod -R +x $APP_HOME/tests
RUN chmod +x $APP_HOME/entrypoint.sh

EXPOSE 2128
EXPOSE 8888
EXPOSE 9042
#EXPOSE 9870
#EXPOSE 4040

#ENTRYPOINT $APP_HOME/entrypoint.sh