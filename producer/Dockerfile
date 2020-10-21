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

################
##### SBT ######
################

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 
RUN apt-get update
RUN yes | apt-get install sbt

################
##### App ######
################

# build
COPY build.sbt $APP_HOME

#update
RUN sbt update

# Copy code
COPY . $APP_HOME

# Compile
RUN sbt compile

# Entrypoint
RUN chmod +x $APP_HOME/entrypoint.sh
ENTRYPOINT $APP_HOME/entrypoint.sh
