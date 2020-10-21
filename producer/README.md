# Data Producer

## Build

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
