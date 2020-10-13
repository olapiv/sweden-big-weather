# ID2221: Final Project

## Introduction

Due to geographical or urban conditions, weather stations are not distributed perfectly equally across countries. For the same reasons, weather stations are not to be found on every inch on a given piece of land. When creating heat maps for countries, however, it is required to calculate a value for every single pixel of the map. Also, since heat maps are meant to show continuous color flows, the calculated temperature values cannot simply assume the temperature of the next weather station but must evaluate temperatures of different weather stations around it. Given live data from multiple weather stations in Sweden, we aim to create a live heat map, where every pixel represents a calculated temperature. We believe this is a good topic, since temperature data is available in abundance and is constantly changing, making it a good use-case for big-data streams.

## Data

- A real-time feed with Open Weather: https://openweathermap.org/api/one-call-api?gclid=CjwKCAjw8MD7BRArEiwAGZsrBZAziHCpMkeqY-7OeABUvfzM1O7ptpy66WoekfnVP_6Cin58VQWMWRoC63IQAvD_BwE

- A real-time feed with Meteo Matics: https://www.meteomatics.com/en/weather-api/?gclid=CjwKCAjw8MD7BRArEiwAGZsrBb_FJfOGWfjdAmOq4jZbyUwXjn2irJ-oHES9DUXtlq6fkQlmz32D8hoCCPwQAvD_BwE

## Useful Links

- Rest API call in Spark: https://stackoverflow.com/questions/41799578/restapi-service-call-from-spark-streaming

## Build

```bash
    docker build -t dic-project .
```

## Start and Setup container

1. Run the Docker container in interactive mode

   ```bash
    docker run -it \
        -p 2128:2128 -p 8888:8888 -p 9042:9042 \
        -v ${PWD}/src:/home/dataintensive/src \
        dic-project:latest
    ```

## Run

1. Run `$APP_HOME/entrypoint.sh`

2. Run `cd src/sparkstreaming/`

    You should now be in the sparkstreaming directory

3. Run `sbt compile`
    This will compile the KafkaSpark application

4. Run `setsid nohup sbt run &`

    This will run the KafkaSpark application in the background

5. Run `cd ..` & then `cd generator`

    You should now be in the generator directory

6. Run `sbt run`

    This will start the Producer to generate a streaming input (pairs of "String,int") and feed them to Kafka.
    Let this run for a while before doing 'ctrl + c' to kill the process/application.
