# Data Producer

## Run & build locally

```bash
    export BROKER_URL=localhost:9092
    export WEATHER_API_TOKEN=f857d120fca3c9d9138f63e95df28464
    sbt run
```

## Build Docker

```bash
    docker build -t dic-producer .
```

## Run Docker

```bash
    docker run \
        -e BROKER_URL=kafka:9092 \
        -e WEATHER_API_TOKEN=f857d120fca3c9d9138f63e95df28464 \
        dic-producer:latest
```
