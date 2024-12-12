import {resolver} from '@rocicorp/resolver';
import {Server} from 'node:http';
import {afterAll, afterEach, beforeAll, describe, expect, test} from 'vitest';
import {WebSocket, WebSocketServer, type RawData} from 'ws';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {randInt} from '../../../../shared/src/rand.js';
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

    installWebSocketHandoff(
      createSilentLogContext(),
      () => ({
        payload: {foo: 'boo'},
        receiver: child,
      }),
      server,
    );

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

  test('double handoff', async () => {
    const [grandParent, parent1] = inProcChannel();
    const [parent2, child] = inProcChannel();

    // server(grandParent) to parent
    installWebSocketHandoff(
      createSilentLogContext(),
      () => ({
        payload: {foo: 'boo'},
        receiver: grandParent,
      }),
      server,
    );

    // parent to child
    installWebSocketHandoff(
      createSilentLogContext(),
      () => ({
        payload: {foo: 'boo'},
        receiver: parent2,
      }),
      parent1,
    );

    // child receives socket
    installWebSocketReceiver(
      wss,
      (ws, payload) => {
        ws.on('message', msg => {
          ws.send(`Received "${msg}" and payload ${JSON.stringify(payload)}`);
          ws.close();
        });
      },
      child,
    );

    const {promise: reply, resolve} = resolver<RawData>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => ws.send('hello'));
    ws.on('message', msg => resolve(msg));

    expect(String(await reply)).toBe(
      'Received "hello" and payload {"foo":"boo"}',
    );
  });

  test('handoff error', async () => {
    installWebSocketHandoff(
      createSilentLogContext(),
      () => {
        throw new Error('fooz barz');
      },
      server,
    );

    const ws = new WebSocket(`ws://localhost:${port}/`);
    const {promise, resolve} = resolver<unknown>();
    ws.on('close', (code, reason) =>
      resolve({code, reason: reason.toString('utf-8')}),
    );

    expect(await promise).toMatchInlineSnapshot(`
      {
        "code": 1002,
        "reason": "Error: fooz barz",
      }
    `);
  });
});
