import http from 'http';
import ws from 'ws';
import express from 'express';
import { kafkaSubscribe, kafkaFakeSubscribe } from './src/consumer.js';
import { setUpClient, setupConsumer } from './src/kafka.js';

const PORT = 8001;
const KAFKA_TOPIC = "city-temperatures"

const app = express();
app.use(express.static('./static'));
const server = http.createServer(app);
const socketServer = new ws.Server({ server: server });

const broadcast = (messageString) => {
    socketServer.clients.forEach(
        (client) => {
            client.send(messageString);
        }
    );
}

const kafkaClient = setUpClient();

kafkaClient.refreshMetadata([KAFKA_TOPIC], (err) => {
    if (err) {
        throw err
    }
    console.log("Refreshed metadata!");

    setupConsumer(kafkaClient, KAFKA_TOPIC, broadcast)

    // This is actually not necessary:
    socketServer.on('connection', socket => {
        console.log("Connected to Websocket!");
        socket.send('Hey there!');
    });

});

server.listen(PORT);
console.log(`socketServer listening: http://localhost:${PORT}`);
