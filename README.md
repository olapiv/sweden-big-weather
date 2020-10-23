# ID2221: Final Project

## Introduction

Due to geographical or urban conditions, weather stations are not distributed perfectly equally across countries. For the same reasons, weather stations are not to be found on every inch on a given piece of land. When creating heat maps for countries, however, it is required to calculate a value for every single pixel of the map. Also, since heat maps are meant to show continuous color flows, the calculated temperature values cannot simply assume the temperature of the next weather station but must evaluate temperatures of different weather stations around it. Given live data from multiple weather stations in Sweden, we aim to create a live heat map, where every pixel represents a calculated temperature. We believe this is a good topic, since temperature data is available in abundance and is constantly changing, making it a good use-case for big-data streams.

## Data

- A real-time feed with Open Weather: https://openweathermap.org/api/one-call-api?gclid=CjwKCAjw8MD7BRArEiwAGZsrBZAziHCpMkeqY-7OeABUvfzM1O7ptpy66WoekfnVP_6Cin58VQWMWRoC63IQAvD_BwE

## Useful Links

- Rest API call in Spark: https://stackoverflow.com/questions/41799578/restapi-service-call-from-spark-streaming

- Websocket in Spark: https://www.nexmo.com/blog/2018/10/15/create-websocket-server-spark-framework-dr

## Build

```bash
    docker-compose build
```

## Start and Setup container

1. Run the Docker container in interactive mode

    ```bash
        docker-compose up
    ```
