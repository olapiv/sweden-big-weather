import http from 'http';
import ws from 'ws';
const { Server, Message, WebSocketClient } = ws;
import express from 'express';
import { kafkaSubscribe } from './src/consumer.js';

const PORT = 8001;

const app = express();

// Server static files
app.use(express.static('./static'));

const server = http.createServer(app);

const socketServer = new Server({ server: server });
socketServer.on('connection', socket => {
    console.log("Connected to Websocket!");
    socket.on('message', message => {
        console.log('Received message: ', message);
    });
    socket.send('Hey there!');

    kafkaSubscribe(
        'testTopic',
        send
    );
});

const send = (message) => {
    socketServer.clients.forEach(
        (client) => {
            client.send(message.value);
        }
    );
}
server.listen(PORT);

console.log(`socketServer listening: http://localhost:${PORT}`);
