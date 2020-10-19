import http from 'http';
import ws from 'ws';
const { Server, Message, WebSocketClient } = ws;
import express from 'express';
import url from 'url';
import { kafkaSubscribe } from './src/consumer.js';

const PORT = 8001;

const app = express();

// Server static files
app.use(express.static('./static'));

const server = http.createServer(app);

const socketServer = new Server({ noServer: true });
socketServer.on('connection', socket => {
    socket.on('message', message => {
        console.log('received: %s', message);
    });
    console.log("Hello!");
    socket.send('something');
});

server.on('upgrade', function upgrade(request, socket, head) {
    const pathname = url.parse(request.url).pathname;
    console.log("pathname: ", pathname);
    if (pathname === '/ws') {
        socketServer.handleUpgrade(request, socket, head, function done(ws) {
            socketServer.emit('connection', ws, request);
      });
    } else {
      socket.destroy();
    }
});

server.listen(PORT);

console.log(`socketServer listening: http://localhost:${PORT}`);
