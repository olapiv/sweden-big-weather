import http from 'http';
import ws from 'ws';
import express from 'express';
import { kafkaSubscribe, kafkaFakeSubscribe } from './src/consumer.js';

const PORT = 8001;

const app = express();

// Server static files
app.use(express.static('./static'));

const server = http.createServer(app);

const socketServer = new ws.Server({ server: server });
socketServer.on('connection', socket => {
    console.log("Connected to Websocket!");
    socket.on('message', message => {
        console.log('Received message: ', message);
    });
    socket.send('Hey there!');

    if (socketServer.clients.size === 1) {
        kafkaFakeSubscribe('city-temperatures', broadcast)
        // kafkaSubscribe('city-temperatures', broadcast);
    }

});

const broadcast = (messageString) => {
    socketServer.clients.forEach(
        (client) => {
            client.send(messageString);
        }
    );
}
server.listen(PORT);

console.log(`socketServer listening: http://localhost:${PORT}`);
