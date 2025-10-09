import {consoleLogSink, LogContext} from '@rocicorp/logger';
import WebSocket from 'ws';

const lc = new LogContext('info', {}, consoleLogSink);

let running = true;
process.on('SIGINT', () => (running = false));
process.on('SIGTERM', () => (running = false));

async function pingLoop(ws: WebSocket) {
  while (running) {
    const ping = Date.now();
    ws.send(JSON.stringify({ping}));
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

const args = process.argv.slice(2);
const port = parseInt(args[0]);

lc.debug?.(`Connecting to server at ${port}`);
const ws = new WebSocket(`ws://localhost:${port}/`);

ws.on('message', (data: Buffer) => {
  const now = Date.now();
  const {ping, pong} = JSON.parse(data.toString()) as {
    ping: number;
    pong: number;
  };
  const received = now - ping;
  lc.info?.({ping, pong, received});
});

ws.on('open', () => {
  lc.debug?.(`Connected to server at ${port}`);
  return pingLoop(ws);
});
