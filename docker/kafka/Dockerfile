FROM dataintensive-base

#########################
##### Kafka #############
#########################

RUN wget https://apache.uib.no/kafka/2.6.0/kafka_2.12-2.6.0.tgz
RUN tar -xvf kafka_2.12-2.6.0.tgz
ENV KAFKA_HOME=$APP_HOME/kafka_2.12-2.6.0
ENV PATH=$KAFKA_HOME/bin:$PATH

# # Entrypoint
ADD ./entrypoint.sh $APP_HOME
RUN chmod +x $APP_HOME/entrypoint.sh
ENTRYPOINT $APP_HOME/entrypoint.sh