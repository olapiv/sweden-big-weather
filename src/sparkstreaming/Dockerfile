FROM dataintensive-base

################
##### SBT ######
################
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 
RUN apt-get update
RUN yes | apt-get install sbt

#########################
##### Apache Spark ######
#########################
RUN wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz 
RUN tar -xvf spark-2.4.3-bin-hadoop2.7.tgz
ENV SPARK_HOME=$APP_HOME/spark-2.4.3-bin-hadoop2.7
ENV PATH=$SPARK_HOME/bin:$PATH


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