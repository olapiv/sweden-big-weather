FROM dataintensive-base

#########################
##### Python 2.7 ########
#########################

RUN apt update
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

EXPOSE 9042

# # Entrypoint
ADD ./entrypoint.sh $APP_HOME
RUN chmod +x $APP_HOME/entrypoint.sh
ENTRYPOINT $APP_HOME/entrypoint.sh