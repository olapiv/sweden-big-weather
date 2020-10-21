FROM dataintensive-base

################
##### SBT ######
################
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 
RUN apt-get update
RUN yes | apt-get install sbt

# Copy code
COPY . $APP_HOME

# Compile
RUN sbt compile

# Entrypoint
RUN chmod +x $APP_HOME/entrypoint.sh
ENTRYPOINT $APP_HOME/entrypoint.sh