import {resolver} from '@rocicorp/resolver';
import {Server} from 'node:http';
import {Worker} from 'node:worker_threads';
import {Queue} from 'shared/src/queue.js';
import {randInt} from 'shared/src/rand.js';
import {afterAll, afterEach, beforeAll, describe, expect, test} from 'vitest';
import WebSocket from 'ws';
import {installWebSocketHandoff} from './websocket-handoff.js';

describe('dispatcher/websocket-handoff', () => {
  let port: number;
  let server: Server;
  let wss: WebSocket.Server;

  beforeAll(() => {
    port = randInt(10000, 20000);
    server = new Server();
    server.listen(port);
    wss = new WebSocket.Server({noServer: true});
  });

  afterEach(() => {
    server.removeAllListeners('upgrade');
  });

  afterAll(() => {
    server.close();
    wss.close();
  });

  test('handoff', async () => {
    const {port1, port2} = new MessageChannel();
    installWebSocketHandoff(server, () => ({
      payload: {foo: 'bar'},
      receiver: port1,
    }));

    const receiver = new Queue<unknown>();
    port2.on('message', msg => receiver.enqueue(msg));

    new WebSocket(`ws://localhost:${port}/`);

    expect(await receiver.dequeue()).toMatchObject({
      fd: expect.any(Number),
      head: expect.any(Uint8Array),
      message: {
        headers: expect.any(Object),
        method: 'GET',
      },
      payload: {
        foo: 'bar',
      },
    });

    // Note: Unfortunately, testing the receiving end in the same thread
    // (i.e. without running it in a Worker) results in an "Error: open EEXIST"
    // error when attempting to create the Socket object with the FileHandle.
    // The receiver-side Socket logic only works in a separate Worker thread.
    // This is tested in the next test.
  });

  test('handoff and reply from worker', async () => {
    const receiver = new Worker(
      INLINED_INSTALL_WEBSOCKET_RECEIVER_FUNCTION +
        `
      const WebSocket = require('ws');
      const {parentPort} = require('node:worker_threads');

      const wss = new WebSocket.Server({noServer: true});

      installWebSocketReceiver(wss, parentPort, (ws, payload) => {
        ws.on('message', msg => {
          ws.send('Received message "' + msg + '" and payload ' + JSON.stringify(payload));
          ws.close();
          wss.close();
        });
      });
      `,
      {eval: true},
    );

    installWebSocketHandoff(server, () => ({
      payload: {foo: 'boo'},
      receiver,
    }));

    const {promise: reply, resolve} = resolver<unknown>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => ws.send('hello'));
    ws.on('message', msg => resolve(msg));

    expect(await reply).toBe(
      'Received message "hello" and payload {"foo":"boo"}',
    );
  });
});

// Loading ESM modules via a Worker is not currently supported by
// the current combination of vitest and Node.
//
// Workaround this by inlining a CommonJS version of code we want to test.
const INLINED_INSTALL_WEBSOCKET_RECEIVER_FUNCTION = `
const {Socket} = require('node:net');

// From websocket-handoff.ts
function installWebSocketReceiver(
  server,
  receiver,
  receive,
) {
  receiver.on('message', msg => {
    const {message, fd, head, payload} = msg;
    const socket = new Socket({
      fd,
      readable: true,
      writable: true,
      allowHalfOpen: true,
    });

    server.handleUpgrade(
      message,
      socket,
      Buffer.from(head),
      ws => receive(ws, payload),
    );
  });
}
  `;
