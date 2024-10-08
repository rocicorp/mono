import {resolver} from '@rocicorp/resolver';
import {Server} from 'node:http';
import {randInt} from '../../../../shared/src/rand.js';
import {afterAll, afterEach, beforeAll, describe, expect, test} from 'vitest';
import {WebSocket, WebSocketServer, type RawData} from 'ws';
import {inProcChannel} from '../../types/processes.js';
import {
  installWebSocketHandoff,
  installWebSocketReceiver,
} from './websocket-handoff.js';

describe('dispatcher/websocket-handoff', () => {
  let port: number;
  let server: Server;
  let wss: WebSocketServer;

  beforeAll(() => {
    port = randInt(10000, 20000);
    server = new Server();
    server.listen(port);
    wss = new WebSocketServer({noServer: true});
  });

  afterEach(() => {
    server.removeAllListeners('upgrade');
  });

  afterAll(() => {
    server.close();
    wss.close();
  });

  test('handoff', async () => {
    const [parent, child] = inProcChannel();

    installWebSocketHandoff(server, () => ({
      payload: {foo: 'boo'},
      receiver: child,
    }));

    installWebSocketReceiver(
      wss,
      (ws, payload) => {
        ws.on('message', msg => {
          ws.send(`Received "${msg}" and payload ${JSON.stringify(payload)}`);
          ws.close();
        });
      },
      parent,
    );

    const {promise: reply, resolve} = resolver<RawData>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => ws.send('hello'));
    ws.on('message', msg => resolve(msg));

    expect(String(await reply)).toBe(
      'Received "hello" and payload {"foo":"boo"}',
    );
  });
});
