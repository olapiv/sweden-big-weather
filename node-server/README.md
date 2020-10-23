# Node Server

This component provides a NodeJS Express app with a http & websocket (ws) server. The http server serves
the webpage for the map, which then connects to the websocket server. The Express app subscribes to a Kafka
topic, so whenever it receives temperature updates, it forwards them to all clients of its websocket. All
temperature points are visualised on a map using D3.js.

## Build

```bash
    npm install
```

## Run

1. Start (npm version: 6.14.7)

    ```bash
        npm start
    ```

2. Visit [localhost:8001](http://localhost:8001) and look at Sweden.

## Build Docker

```bash
    docker build -t dic-node-server .
```

## Run Docker

1. Start

    ```bash
        docker run -p 8001:8001 dic-node-server:latest
    ```

2. Visit [localhost:8001](http://localhost:8001) and look at Sweden.
